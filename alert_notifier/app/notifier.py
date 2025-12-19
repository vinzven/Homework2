import smtplib
from email.message import EmailMessage

def send_email(to, subject, body):
    msg = EmailMessage()
    msg["From"] = "alerts@dsbd.local"
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(body)

    # Configurare il tuo SMTP server
    with smtplib.SMTP("smtp", 25) as s:
        s.send_message(msg)
