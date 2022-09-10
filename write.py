from __future__ import annotations

import itertools
import dataclasses
import sqlite3
import csv
from collections import defaultdict
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

SQL = """
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


@dataclasses.dataclass
class Spec:
    subtitle: str
    title: str
    out_file: str
    limit: int | None = 10000
    col: str | None = None
    offset: int | None = None

    @property
    def column(self):
        return self.col or self.out_file


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


class Divider:
    SPECS = (
        Spec('Tue 13th Sep, 15:00, in front of A03', 'Brno Observatory and Planetarium', 'observatory', 140),
        Spec('Tue 13th Sep, 19:40, in front of A03', 'Quiz Night #1', 'quiz1', 42, 'quiz_night', 0),

        Spec('Wed 14th Sep, 14:00, in front of A03', 'Panda Games', 'panda_games', 1000),
        Spec('Wed 14th Sep, 18:50, in front of A03', 'Board Games #1', 'board1', 50, 'board_games', 0),
        Spec('Wed 14th Sep, 19:00, Panda Point', 'Guitar Jam', 'guitar_jam', 30),

        Spec('Thu 15th Sep, 14:00, in front of A03', 'Technical Museum', 'technical_museum', 140),
        Spec('Thu 15th Sep, 18:50, in front of A03', 'Board Games #2', 'board2', 50, 'board_games', 50),
        Spec('Thu 15th Sep, 19:00, in front of A03', 'Bowling Time', 'bowling', 36),

        Spec('Fri 16th Sep, 15:00, in front of A03', 'VIDA! Science centre', 'vida', 140),
        Spec('Fri 16th Sep, 19:40, in front of A03', 'Quiz Night #2', 'quiz2', 42, 'quiz_night', 42),

        Spec('Sat 17th Sep, 14:00, in front of A03', 'Erasmus Festival and Flag Parade', 'erasmus_festival', 1000),

        Spec('Sun 18th Sep, 12:00, in front of A03', 'Brno City Rally', 'city_rally', 136),
        Spec('Sun 18th Sep, 20:00, in front of A03', 'BBQ', 'bbq', 1000),
    )

    # planetarium vida museum city_rally

    NO_CONFICT = (
        ('board1', 'guitar_jam'),
        ('board2', 'bowling'),
        # ('movie1', 'board4'),
    )

    people_to_events = defaultdict(list)
    people_to_email = defaultdict(list)
    events_to_people_email = defaultdict(set)
    email_to_pp_time = dict()

    def __init__(self, db_path: Path = './ww.sqlite'):
        db = sqlite3.connect(db_path)

        self.cursor = db.cursor()
        self.base_dir = Path('./out')
        self.base_dir.mkdir(exist_ok=True)

        self.html_path = (self.base_dir / f'ALL.html').as_posix()
        self.pdf_path = (self.base_dir / f'ALL.pdf').as_posix()

    def write_event_detail(self, data: list[tuple[str, ...]], spec: Spec):
        csv_path = (self.base_dir / f'{spec.out_file}.csv').as_posix()
        with open(csv_path, 'w') as ofile:
            writer = csv.writer(ofile, dialect='excel')

            writer.writerows(data)
        pdf_path = (self.base_dir / f'{spec.out_file}.pdf').as_posix()
        html_path = (self.base_dir / f'{spec.out_file}.html').as_posix()
        template = Engine().from_string(open('single_activity_pdf.html').read())
        context = Context({
            'rows': tuple(tuple(
                v.lower().title() for v in row
            ) for row in data),
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
        for spec in self.SPECS:
            self.cursor.execute(SQL.format(
                column=spec.column,
                limit=f"LIMIT {spec.limit}"
                if not spec.offset else
                f"LIMIT {spec.limit} OFFSET {spec.offset}"
            ))
            loaded = self.cursor.fetchall()

            print(spec.out_file, len(loaded))

            for row in loaded:
                (time, first, last, phone, email, *_) = row
                self.people_to_events[(first, last)].append(spec.title)

                self.events_to_people_email[spec.out_file] |= {email}
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
                time,
            )
            i += 1

            if i % 30 == 0:
                yield '#', '', '', '', '#' * 4

    def run(self):
        self.divide()

        self.print_conflict()

        # cursor.execute("SELECT email FROM data WHERE czech='YES' ORDER by time LIMIT 80")
        MOVE_TO_LATER: set[str] = set()  # set(t[0] for t in cursor.fetchall())
        sooner = {key: events for key, events in self.people_to_events.items() if
                  self.people_to_email[key] not in MOVE_TO_LATER}

        later = {key: events for key, events in self.people_to_events.items() if
                 self.people_to_email[key] in MOVE_TO_LATER}

        context = Context({
            'people': list(itertools.chain(
                self.people_to_data(sooner, "11:30 12:00 12:30 13:00 13:30 14:00 14:30 15:00 15:30 16:00 16:30".split(' ')),
                # (('#', '', '', '', '#' * 4),),
                # people_to_data(later, "16:00 16:30 17:00".split(' '), len(sooner)),
            ))
        })

        with open(self.html_path, 'w') as ofile:
            ofile.write(ALL_ACTIVITIES_TMPL.render(context))

        pdfkit.from_file(self.html_path, self.pdf_path)

        events_to_spec = dict()

        for spec in self.SPECS:
            events_to_spec[spec.title] = spec

        messages = []
        for key, events in tuple(self.people_to_events.items()):
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
    divider.run()
