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
Panda Games
Board Games #1
Board Games #2
Ice skating
Quiz Night #1
Board Games #3
Brno Observatory and Planetarium
Movie Night #1
Board Games #4
VIDA! Science centre
Quiz Night #2
Welcome trip to Znojmo city
Brno City Rally: Let's discover historic centre of Brno!
Movie Night #2
"""

SPECS = (
    Spec('Tue 1st Feb, 14:00, in front of A03', 'Panda Games', 'panda_games', 1000),
    Spec('Tue 1st Feb, 18:50, in front of A03', 'Board Games #1', 'board1', 40, 'board_games', 0),
    Spec('Tue 1st Feb, 19:30, Panda Point', 'Board Games #2', 'board2', 25, 'board_games', 40),

    Spec('Wed 2nd Feb, 14:00, in front of A03', 'Ice skating', 'ice_skating_filtered', 1000),
    Spec('Wed 2nd Feb, 19:00, in front of A03', 'Quiz Night #1', 'quiz1', 42, 'quiz_night', 0),
    Spec('Wed 2nd Feb, 19:30, Panda Point', 'Board Games #3', 'board3', 25, 'board_games', 65),

    Spec('Thu 3rd Feb, 16:15, in front of A03', 'Brno Observatory and Planetarium', 'observatory', 100),
    Spec('Thu 3rd Feb, 20:00, Panda Point', 'Movie Night #1', 'movie1', 25, 'movie_night', 0),
    Spec('Thu 3rd Feb, 18:50, in front of A03', 'Board Games #4', 'board4', 40, 'board_games', 90),

    Spec('Fri 4th Feb, 15:00, in front of A03', 'VIDA! Science centre', 'vida', 100),
    Spec('Fri 4th Feb, 19:30, in front of A03', 'Quiz Night #2', 'quiz2', 42, 'quiz_night', 42),

    Spec('Sat 5th, 8:00, in front of A03', 'Welcome trip to Znojmo city', 'welcome_trip', 82),

    Spec('Sun 6th Feb, 12:00, in front of A03', 'Brno City Rally', 'city_rally', 120),
    Spec('Sun 6th Feb, 20:00, Panda Point', 'Movie Night #2', 'movie2', 25, 'movie_night', 25),
)

SINGLE_ACTIVITY_TEMPLATE = """
<!doctype html>
<html lang='en'>
<head>
    <meta charset='UTF-8'>
    <style>
        body {
        font-family: 'Lato', sans-serif;
    }
        h1, h2, h3 {text-align: center; font-family: 'Kelson Sans', serif; font-weight: 600}

    table {
        width: 100%;
    }
    table, th, td {
        border: 1px solid black;
        border-collapse: collapse;
        line-height: 24px;
    }
    tr td:nth-child(5), tr th:nth-child(5) {
        width: 35%;
    }
    thead { display: table-header-group; }
    tfoot { display: table-row-group; }
    tr { page-break-inside: avoid; }
    h1, h3 {text-align: center}
    td { padding: 0 4px }
    </style>
</head>
<body>
<h1>{{ spec.title }}</h1>
<h2>{{ spec.subtitle }}</h2>
<h3>Welcome Week Summer 2022</h3>
<table>
<thead>
    <tr>
        <th>#</th>
        <th>First name</th>
        <th>Last name</th>
        <th>Phone number</th>
        <th>Signature</th>
    </tr>
</thead>
<tbody>
    {% for row in rows %}
        <tr>
            <td align='center'>{{ forloop.counter }}</td>
            <td>{{ row.1 }}</td>
            <td>{{ row.2 }}</td>
            <td>{{ row.3 }}</td>
            <td>&nbsp;</td>
        </tr>
    {% endfor %}
</tbody>
</table>
</body>
</html>
"""

people_to_events = defaultdict(list)
people_to_email = defaultdict(list)
events_to_people_email = defaultdict(set)

db = sqlite3.connect('./ww.sqlite3')

cursor = db.cursor()
BASE_DIR = Path('./out')
BASE_DIR.mkdir(exist_ok=True)

NO_CONFICT = (
    ('board1', 'board2'),
    ('quiz1', 'board3'),
    ('movie1', 'board4'),
)


def write_event_detail(data):
    csv_path = (BASE_DIR / f'{spec.out_file}.csv').as_posix()
    with open(csv_path, 'w') as ofile:
        writer = csv.writer(ofile, dialect='excel')

        writer.writerows(data)
    pdf_path = (BASE_DIR / f'{spec.out_file}.pdf').as_posix()
    html_path = (BASE_DIR / f'{spec.out_file}.html').as_posix()
    template = Engine().from_string(SINGLE_ACTIVITY_TEMPLATE)
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


for spec in SPECS:
    cursor.execute(SQL.format(
        column=spec.column,
        limit=f"LIMIT {spec.limit}"
        if not spec.offset else
        f"LIMIT {spec.limit} OFFSET {spec.offset}"
    ))
    loaded = cursor.fetchall()

    print(spec.out_file, len(loaded))

    for row in loaded:
        (time, first, last, phone, email, *_) = row
        people_to_events[(first, last)].append(spec.title)

        events_to_people_email[spec.out_file] |= {email}
        people_to_email[(first, last)] = email

    write_event_detail(loaded)

for g1, g2 in NO_CONFICT:
    intersect = events_to_people_email[g1] & events_to_people_email[g2]
    if intersect:
        print(f'CONFLICT: {g1}&{g2}: {intersect}')

ALL_ACTIVITIES_TEMPLATE = """
<!doctype html>
<html lang='en'>
<head>
    <meta charset='UTF-8'>
    <style>
    body {
        font-family: 'Lato', sans-serif;
    }
h1, h2, h3 {text-align: center; font-family: 'Kelson Sans', serif; font-weight: 600}
    table {
        width: 100%;
    }
    table, th, td {
        border: 1px solid black;
        border-collapse: collapse;
        line-height: 24px;
    }
    thead { display: table-header-group; }
    tfoot { display: table-row-group; }
    
    td { padding: 0 4px }
    td.activites {
        line-height: 16px;
        font-size: .6em;
    }
    .activites {width: 55%}
    </style>
</head>
<body>
<h1>Panda Point Schedule: 31st Jan</h1>
<h3>Welcome Week Summer 2022 | ESN VUT Brno</h3>
<table>
<thead>
    <tr>
        <th>#</th>
        <th>First name</th>
        <th>Last name</th>
        <th>Events</th>
        <th>Time</th>
    </tr>
</thead>
<tbody>
    {% for i, first, last, activities, time in people %}
        <tr>
            <td align='center'>{{ i }}</td>
            <td>{{ first }}</td>
            <td class='last'>{{ last }}</td>
            <td class='activites'>{{ activities }}</td>
            <td align='center'><strong>{{ time }}</strong></td>
        </tr>
    {% endfor %}
</tbody>
</table>
</body>
</html>
"""

html_path = (BASE_DIR / f'ALL.html').as_posix()
pdf_path = (BASE_DIR / f'ALL.pdf').as_posix()

template = Engine().from_string(ALL_ACTIVITIES_TEMPLATE)

email_to_pp_time = dict()

cursor.execute("SELECT email FROM data WHERE czech='YES' ORDER by time LIMIT 80")
MOVE_TO_LATER: set[str] = set(t[0] for t in cursor.fetchall())


def people_to_data(_data, times, count_from=0):
    global email_to_pp_time

    i = 0
    for (key, activities) in sorted(_data.items(), key=lambda t: t[0][1]):
        email_to_pp_time[people_to_email[key]] = time = times[i // 30]

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


sooner = {key: events for key, events in people_to_events.items() if people_to_email[key] not in MOVE_TO_LATER}
later = {key: events for key, events in people_to_events.items() if people_to_email[key] in MOVE_TO_LATER}

context = Context({'people': itertools.chain(
    people_to_data(sooner, "11:30 12:00 12:30 13:00 13:30".split(' ')),
    (('#', '', '', '', '#' * 4),),
    people_to_data(later, "16:00 16:30 17:00".split(' '), len(sooner)),
)})

with open(html_path, 'w') as ofile:
    ofile.write(template.render(context))

pdfkit.from_file(html_path, pdf_path)

events_to_spec = dict()

for spec in SPECS:
    events_to_spec[spec.title] = spec

messages = []
for key, events in tuple(people_to_events.items()):
    first, last = key

    email = people_to_email[key]
    time = email_to_pp_time[email]

    print(f'{i};{first};{last};{email}')

    template = Engine().from_string(open('mail_template.html').read())
    html_message = template.render(Context({
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
    mail.attach_file('./panda-point-open-hours-welcome-week-summer-2022.png')
    messages.append(mail)

    # mail.send(fail_silently=False)

    # mailer.send_messages(messages)
