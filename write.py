from __future__ import annotations

import itertools
import dataclasses
import sqlite3
import csv
from collections import defaultdict, namedtuple
from dataclasses import dataclass
from operator import itemgetter
from pathlib import Path

import pdfkit
from django.template import Context, Engine
from django.conf import settings
from django.core.mail.backends.smtp import EmailBackend as Smtp
from django.core.mail import EmailMultiAlternatives
from django.utils.html import strip_tags

settings.configure()
mailer = Smtp(
    host='smtp.gmail.com',
    port=465,
    username='...',
    use_ssl=True,
    password='...'
)

SINGLE_EVENT_QUERY = """
WITH ordered AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY email ORDER BY time DESC) = 1 as is_last
    FROM data
)
SELECT
       o.time,
       o.first_name,
       o.last_name,
       o.phone_number,
       o.email,
       o.{column}
FROM ordered o
WHERE is_last and {column} = 'YES'
ORDER BY time ASC
{limit};

"""

ALL_DATA_QUERY = """
WITH ordered AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY email ORDER BY time DESC) = 1 as is_last
    FROM data
)
SELECT
       o.*
FROM ordered o
WHERE is_last
ORDER BY time ASC;
"""


@dataclasses.dataclass
class Event:
    subtitle: str
    title: str
    identifier: str
    limit: int | None = 10000
    db_column: str | None = None
    offset: int | None = None

    @property
    def column(self) -> str:
        return self.db_column or self.identifier

    @property
    def category(self):
        return self.column


"""
observatory
quiz 1
panda
board 1
guitar
museum
board 2
bowling
vida
quiz 2
festival
city rally
bbq
"""

ALL_ACTIVITIES_TMPL = Engine().from_string(open('all_activities_pdf.html').read())
MAIL_TMPL = Engine().from_string(open('mail_template.html').read())


def row_factory(cursor, row):
    return dict(zip(map(itemgetter(0), cursor.description), row))


class Divider:
    EVENTS = (
        Event('Tue 13th Sep, 15:00, in front of A03', 'Brno Observatory and Planetarium', 'observatory', 140),
        Event('Tue 13th Sep, 19:40, in front of A03', 'Quiz Night #1', 'quiz1', 42, 'quiz_night', 0),

        Event('Wed 14th Sep, 14:00, in front of A03', 'Panda Games', 'panda_games', 1000),
        Event('Wed 14th Sep, 18:50, in front of A03', 'Board Games #1', 'board1', 50, 'board_games', 0),
        Event('Wed 14th Sep, 19:00, Panda Point', 'Guitar Jam', 'guitar_jam', 30),

        Event('Thu 15th Sep, 14:00, in front of A03', 'Technical Museum', 'technical_museum', 140),
        Event('Thu 15th Sep, 18:50, in front of A03', 'Board Games #2', 'board2', 50, 'board_games', 50),
        Event('Thu 15th Sep, 19:00, in front of A03', 'Bowling Time', 'bowling', 36),

        Event('Fri 16th Sep, 15:00, in front of A03', 'VIDA! Science centre', 'vida', 140),
        Event('Fri 16th Sep, 19:40, in front of A03', 'Quiz Night #2', 'quiz2', 42, 'quiz_night', 42),

        Event('Sat 17th Sep, 14:00, in front of A03', 'Erasmus Festival and Flag Parade', 'erasmus_festival', 1000),

        Event('Sun 18th Sep, 12:00, in front of A03', 'Brno City Rally', 'city_rally', 136),
        Event('Sun 18th Sep, 20:00, in front of A03', 'BBQ', 'bbq', 1000),
    )

    # planetarium vida museum city_rally

    NO_CONFICT = (
        {'board1', 'guitar_jam'},
        {'board2', 'bowling'},
        # ('movie1', 'board4'),
    )

    people_to_event_titles = defaultdict(list)
    people_to_email = defaultdict(list)
    events_to_people_email = defaultdict(set)
    email_to_pp_time = dict()

    people_to_wanted_events_count = dict()

    def fair_divide(self):
        self.cursor.execute(ALL_DATA_QUERY)
        data = tuple(self.cursor.fetchall())
        # people_by_event_category = {
        #     e.identifier: tuple(filter(lambda t: t[e.identifier], data))
        #     for e in self.EVENTS
        # }

        MAX_TWO_FROM = {*'observatory vida technical_museum city_rally'.split(' ')}

        people_by_events = defaultdict(list)
        event_people_count = defaultdict(int)
        people_to_event_identifiers = defaultdict(set)

        for person in data:
            # print(person['email'])
            assigned_to_event_categories = set()
            assigned_to_events = set()

            self.people_to_wanted_events_count[(person['first_name'], person['last_name'])] = sum(
                person[e.column] == 'YES' for e in self.EVENTS
            )

            for e in self.EVENTS:
                # does he really want this event?
                if person[e.column] != 'YES':
                    continue

                # candidate, but cannot have more than two events
                if len(MAX_TWO_FROM & people_to_event_identifiers[person['email']]) >= 2:
                    continue

                # already does have this one in this category
                if e.category in assigned_to_event_categories:
                    continue

                # new event would be a conflict?
                if any({*assigned_to_events, e.identifier}.issuperset(c) for c in self.NO_CONFICT):
                    continue

                # so fine, assign him
                people_by_events[e.identifier].append(person)
                event_people_count[e.identifier] += 1
                people_to_event_identifiers[person['email']].add(e.identifier)
                assigned_to_event_categories.add(e.category)
                assigned_to_events.add(e.identifier)

                self.people_to_event_titles[(person['first_name'], person['last_name'])].append(e.title)
                self.events_to_people_email[e.identifier].add(person['email'])
                self.people_to_email[(person['first_name'], person['last_name'])] = person['email']

            if not assigned_to_events:
                print(f'no.event', person['email'], ' '.join(f'{e.identifier}={person[e.column]}' for e in self.EVENTS))

        for e in self.EVENTS:
            self.write_event_detail(people_by_events[e.identifier], e)

    def __init__(self, db_path: Path = './ww.sqlite'):
        conn = sqlite3.connect(db_path)

        conn.row_factory = row_factory

        self.cursor = conn.cursor()
        self.base_dir = Path('./out')
        self.base_dir.mkdir(exist_ok=True)

        self.html_path = (self.base_dir / f'ALL.html').as_posix()
        self.pdf_path = (self.base_dir / f'ALL.pdf').as_posix()

    def write_event_detail(self, data: list[dict[str, str]], spec: Event):
        # csv_path = (self.base_dir / f'{spec.identifier}.csv').as_posix()
        # with open(csv_path, 'w') as ofile:
        #     writer = csv.writer(ofile, dialect='excel')
        #
        #     writer.writerows(data)

        pdf_path = (self.base_dir / f'{spec.identifier}.pdf').as_posix()
        html_path = (self.base_dir / f'{spec.identifier}.html').as_posix()
        template = Engine().from_string(open('single_activity_pdf.html').read())
        context = Context({
            'rows': tuple(
                (
                    person['first_name'],
                    person['last_name'],
                    person['phone_number'],
                ) for person in data
            ),
            'spec': spec
        })
        with open(html_path, 'w') as ofile:
            ofile.write(template.render(context))
        pdfkit.from_file(html_path, pdf_path, {
            'footer-left': f'{spec.title}',
            'footer-right': f'{spec.subtitle}',
            'footer-font-name': 'Lato',
            'footer-center': 'ESN VUT Brno',
        })

    def divide(self):
        for spec in self.EVENTS:
            self.cursor.execute(SINGLE_EVENT_QUERY.format(
                column=spec.column,
                limit=f"LIMIT {spec.limit}"
                if not spec.offset else
                f"LIMIT {spec.limit} OFFSET {spec.offset}"
            ))
            loaded = self.cursor.fetchall()

            print(spec.identifier, len(loaded))

            for row in loaded:
                (time, first, last, phone, email, *_) = row
                self.people_to_event_titles[(first, last)].append(spec.title)

                self.events_to_people_email[spec.identifier] |= {email}
                self.people_to_email[(first, last)] = email

            self.write_event_detail(loaded, spec)

    def print_conflict(self):
        for g1, g2 in self.NO_CONFICT:
            intersect = self.events_to_people_email[g1] & self.events_to_people_email[g2]
            if intersect:
                print(f'CONFLICT: {g1}&{g2}: {intersect}')

    def people_to_data(self, _data, times, count_from=0):
        i = 0
        for (key, activities) in sorted(_data.items(), key=lambda t: t[0][1]):
            self.email_to_pp_time[self.people_to_email[key]] = time = times[i // 30]

            yield (
                count_from + i + 1,
                key[0].title(),
                key[1].title(),
                ', '.join(activities),
                f'{len(activities)}/{self.people_to_wanted_events_count[key]}',
                time,
            )
            i += 1

            if i % 30 == 0:
                yield '#', '', '', '', '', '#' * 4

    def run(self):

        self.print_conflict()

        # cursor.execute("SELECT email FROM data WHERE czech='YES' ORDER by time LIMIT 80")
        MOVE_TO_LATER: set[str] = set()  # set(t[0] for t in cursor.fetchall())
        sooner = {key: events for key, events in self.people_to_event_titles.items() if
                  self.people_to_email[key] not in MOVE_TO_LATER}

        later = {key: events for key, events in self.people_to_event_titles.items() if
                 self.people_to_email[key] in MOVE_TO_LATER}

        render_data = list(itertools.chain(
            self.people_to_data(sooner,
                                "11:30 12:00 12:30 13:00 13:30 14:00 14:30 15:00 15:30 16:00 16:30".split(' ')),
            # (('#', '', '', '', '#' * 4),),
            # people_to_data(later, "16:00 16:30 17:00".split(' '), len(sooner)),
        ))
        context = Context({'people': render_data})

        csv_path = (self.base_dir / f'ALL.csv').as_posix()
        with open(csv_path, 'w') as ofile:
            writer = csv.writer(ofile, dialect='excel')
            writer.writerow(('id', 'first_name', 'last_name', 'activities', 'count', 'time'))
            writer.writerows([d for d in render_data if d[0] != '#'])

        with open(self.html_path, 'w') as ofile:
            ofile.write(ALL_ACTIVITIES_TMPL.render(context))

        pdfkit.from_file(self.html_path, self.pdf_path)

        events_to_spec = dict()

        for spec in self.EVENTS:
            events_to_spec[spec.title] = spec

        messages = []
        for key, events in tuple(self.people_to_event_titles.items()):
            first, last = key

            email = self.people_to_email[key]
            time = self.email_to_pp_time[email]

            print(f'{key};{first};{last};{email}')

            html_message = MAIL_TMPL.render(Context({
                'events': ((events_to_spec[e].title, events_to_spec[e].subtitle) for e in events),
                'first': first,
                'time': time,
            }))

            plain_message = strip_tags(html_message)

            mail = EmailMultiAlternatives(
                subject='Welcome Week Events | IMPORTANT | ESN VUT Brno',
                from_email='Events Manager ESN VUT Brno <events@esnvutbrno.cz>',
                to=(
                    # 'President <prezident@esnvutbrno.cz>',
                    # 'Joe <events@esnvutbrno.cz>',
                    # 'Viceprezident <vice@esnvutbrno.cz>',
                    # f'{email}',
                ),  # !!!
                body=str(plain_message),
                bcc=['president@esnvutbrno.cz'],
                connection=mailer,
            )
            mail.attach_alternative(html_message, 'text/html')
            # mail.attach_file('./panda-point-open-hours-welcome-week-summer-2022.png')
            messages.append(mail)

            # mail.send(fail_silently=False)

            # mailer.send_messages(messages)


if __name__ == '__main__':
    divider = Divider()

    # divider.divide()
    divider.fair_divide()
    divider.run()
