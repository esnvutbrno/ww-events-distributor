#!/bin/env python3
from django.core.mail import EmailMultiAlternatives
from django.template import Engine, Context
from django.utils.html import strip_tags

from django.conf import settings
from django.core.mail.backends.smtp import EmailBackend as Smtp

settings.configure()
mailer = Smtp(
    host='smtp.gmail.com',
    port=465,
    username='...',
    use_ssl=True,
    password='...'
)

with open('done-cards.tsv', 'r') as f:
    data = [(*d.split('\t'),) for d in f.readlines()]

template = Engine().from_string(open('esncard_ready_template.html').read())

messages = []

for (email, name) in data:
    print(email, name)
    html_message = template.render(Context({'name': name}))

    plain_message = strip_tags(html_message)

    mail = EmailMultiAlternatives(
        subject='Your ESNcard is ready | ESN VUT Brno',
        from_email='Events Manager ESN VUT Brno <events@esnvutbrno.cz>',
        to=(
            # 'President <prezident@esnvutbrno.cz>',
            # 'Joe <events@esnvutbrno.cz>',
            # 'Viceprezident <vice@esnvutbrno.cz>',
            f'{email}',
        ),  # !!!
        body=str(plain_message),
        connection=mailer,
    )
    mail.attach_alternative(html_message, 'text/html')
    mail.attach_file('./panda-point-open-hours-welcome-week-summer-2022.png')
    messages.append(mail)


# mailer.send_messages(messages, )
