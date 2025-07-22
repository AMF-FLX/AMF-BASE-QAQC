import os
import smtplib

from configparser import ConfigParser
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

__author__ = 'You-Wei Cheah, Sy-Toan Ngo'

# This code is a modified version of AMF-Utils


class MailerException(Exception):
    def __init__(self, error_message, log):
        log.error(error_message)
        super().__init__(error_message)


class Mailer:
    def __init__(self, log, config_file='qaqc.cfg'):
        self.log = log
        config = ConfigParser()
        self.host = None
        config_path = os.path.join(os.getcwd(), config_file)
        with open(config_path, 'r') as cfg:
            config.read_file(cfg)
            cfg_section = 'AMP'
            if config.has_section(cfg_section):
                self.host = config.get(cfg_section, 'host')
        if not self.host:
            err_msg = f'No host specified in {config_path}'
            raise MailerException(err_msg, self.log)

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
