"""Notification Client file"""
import sys
import logging
sys.path.append("..")
from trigger_email import SendEmail
from db_connector import DB_Connector

class NotificationClient:
    """
    The Class provides methods to initiate SMTP emails \
        and query Hot Path Database for threshold breaches
    """

    layer = None
    frequency = None
    email = None
    query_frequency = None
    dag_id = None

    def __init__(self):
        self.logger = logging.getLogger('master')
        try:
            self.connection = DB_Connector().connect_to_psql()
        except Exception:
            raise

    def query_executor(self):
        """The function executes the layer threshold breach \
                    query and returns the list object"""
        try:
            res = None
            select_query =  f"select kpi, count(kpi) from \
                detail_event.app_{self.layer}_notification where \
                timestamp > NOW()-INTERVAL '{self.frequency} minute' group by kpi"
            with self.connection:
                with self.connection.cursor() as cur:
                    cur.execute(select_query)
                    res = cur.fetchall()
            return res
        except Exception:
            raise

    def send_email(self, layer_threshold_result):
        """The function initiates the email send\
                    once threshold breach is available"""
        try:
            self.logger.info("Initiating Email Send")
            SendEmail().smtp_email_trigger(layer_threshold_result,\
                self.email, self.layer, self.dag_id, self.frequency)
        except Exception:
            raise

    def start_process(self, layer, frequency, email, dag_id):
        """The function initiates values required for sending \
                    email, queries the DB and initiates the mail"""
        self.layer = layer
        self.frequency = frequency
        self.email = email
        self.dag_id = dag_id

        try:
            layer_threshold_result = self.query_executor()
            if layer_threshold_result:
                self.send_email(layer_threshold_result)
            else:
                raise TypeError
        except Exception:
            raise

