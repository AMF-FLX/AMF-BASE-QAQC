import os
import smtplib

from configparser import ConfigParser
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

__author__ = 'You-Wei Cheah, Sy-Toan Ngo'

class MailerException(Exception):
    def __init__(self, error_message):
        _log.error(error_message)
        super().__init__(error_message)


class Mailer:
    def __init__(self, log):
        self.log = log
        config = ConfigParser()
        self.host = None
        with open(os.path.join(os.getcwd(), 'qaqc.cfg'), 'r') as cfg:
            config.read_file(cfg)
            cfg_section = 'AMP'
            if config.has_section(cfg_section):
                self.host = config.get(cfg_section, 'host')
        if not self.host:
            err_msg = f'No host specified in config'
            raise MailerException(err_msg)

    def build_multipart_text_msg(
            self, sender: str, receipients: list,
            subject: str, body: str, cc: list = None):
        msg = MIMEMultipart()
        msg['From'] = sender
        msg['To'] = ', '.join(receipients)
        if cc:
            msg['Cc'] = ', '.join(cc)
        msg['Subject'] = subject
        msg.attach(MIMEText(body))
        return msg

    def send_mail(self, sender: str, receipients: list, msg: MIMEMultipart):
        with smtplib.SMTP(self.host) as mailer:
            mailer.starttls()
            mailer.sendmail(sender, receipients, msg.as_string())
            status = mailer.quit()
            self.log.info(status)