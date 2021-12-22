import json
import smtplib
import ssl


port = 587  # For starttls
smtp_server = "smtp.gmail.com"
sender_email = "adevelopment705@gmail.com"
receiver_email = "adevelopment705@gmail.com"
# password = input("Type your password and press enter:")
password = 'demodevelopment'
message = """\
Subject: ASTEROID ALERT
Alert Data=
"""


def send_alert(asteroid_alert):
    copied = asteroid_alert.copy()
    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, port) as server:
        server.ehlo()  # Can be omitted
        server.starttls(context=context)
        server.ehlo()  # Can be omitted
        server.login(sender_email, password)
        copied.pop('method', None)
        copied.pop('response_stream_name', None)
        copied.pop('request_id', None)
        msg = f'{message}{json.dumps(copied)}'
        server.sendmail(sender_email, receiver_email, msg)
