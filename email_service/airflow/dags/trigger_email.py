import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
import traceback
import pandas as pd
from pretty_html_table import build_table
from smtplib import SMTPException

class SendEmail:
        port = 587
        smtp_server = "smtp.gmail.com"
        sender_email = "smtp.eas@gmail.com"
        password = "ster#@!21"

        def smtp_email_trigger(self, layer_threshold_result, email, layer, dag_id, timeframe):
                context = ssl.create_default_context()
                msg = MIMEMultipart()
                msg['Subject'] = f':::EAS Alert:::'
                
                df_data = pd.DataFrame(layer_threshold_result, columns=["Key Performance Indicator (KPI)", "Breach Count"])
                df_table = build_table(df_data, 'blue_light')

                body = f"Hi,\n\nPlease find below the EAS report:\n\nDag_ID: {dag_id.upper()}\nLayer: {layer.upper()}\
                \nFrequency: {timeframe} minutes\n\nTable Contents:\n"
                msg.attach(MIMEText(body,'plain'))
                msg.attach(MIMEText(df_table, 'html'))

                with smtplib.SMTP(self.smtp_server, self.port) as server:
                        try:
                                server.starttls(context=context)
                                server.login(self.sender_email, self.password)
                                server.sendmail(self.sender_email, email, msg.as_string())
                                print(f'EAS::: Sent Mail Successfully to {email}')
                        except SMTPException:
                                raise
