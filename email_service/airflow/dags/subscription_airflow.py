import os
from datetime import timedelta
import re
from datetime import datetime
from datetime import date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2
import logging
from config import config
from notification_client import Notification_Client

logger = logging.getLogger("airflow.task")
database_parameters = config()
CONNECTION = psycopg2.connect(**database_parameters)

class STLDB2DAG:
    def __init__(self):
        '''Initialize db connections and default arguments'''

        try:
            self.cursor = CONNECTION.cursor()
        except Exception as exp:
            logger.exception("Exception in DagCreator, {}".format(str(exp)))


    def get_id(self, rec):
        """Function to retrieve task id in uuid format"""
        characters_to_remove = "(),"
        for character in characters_to_remove:
            rec = rec.replace(character, "")
        return rec

    def getdata(self, task_id):
        """Function to retrieve data from TSDB for DAG creation"""
        thisdict={}
        try:
            #Query to fetch data from TSDB app_batch_processing table
            postgreSQL_select_Query = "select * from eas_master where uuid = "+task_id
            self.cursor.execute(postgreSQL_select_Query)
            records = self.cursor.fetchall()
            for row in records:
                thisdict['uuid'] = row[0]
                thisdict['email'] = row[1]
                thisdict['dag_name'] = row[2]
                thisdict['start_date'] = row[5]
                thisdict['end_date'] = row[6]
                thisdict['frequency'] = row[4]
                thisdict['layer'] = row[3]
            return thisdict
        except (Exception, psycopg2.Error) as error:
            #Throw exception if connection to databse doesn't established
            #logger.exception("Error while fetching data from PostgreSQL", error)
            return None

    def main(self, **kwargs):
        """Main Function to be executed when DAG is triggered from UI"""
        print("HELLO", kwargs)
        Notification_Client().start_process(kwargs.get('layer'),kwargs.get('frequency'),kwargs.get('email'),kwargs.get('dag_name'))
        print("Successfully Executed!")
        #self.cursor.execute()
        #Milan email smtp function

    def create_dag(self, dag_id,
                schedule,
                default_args, table_data):
        """Creating DAGs dynamically according to table data"""
        try:
            with DAG(
                    dag_id,
                    default_args=default_args,
                    schedule_interval=schedule,
                    is_paused_upon_creation=False
                    ) as dag:
                task_id=dag_id
                Export_Operator = PythonOperator(
                        task_id=task_id,
                        python_callable=self.main,
                        op_kwargs=table_data,
                        dag=dag)
                globals()[dag_id] = dag

        except (Exception, psycopg2.DatabaseError) as exp:
            logger.exception("Error while loading PostgreSQL table", exp)

    def get_schedule(self, freq):
        if freq==10:
            frequency='*/10 * * * *'
        elif freq==15:
            frequency='*/15 * * * *'
        else:
            frequency='*/25 * * * *'
        return frequency

    def retrieve_id(self):
        get_query="SELECT uuid FROM eas_master where status='ACTIVE'"
        self.cursor.execute(get_query)
        result=self.cursor.fetchall()
        
        end_query="SELECT uuid,end_date from eas_master where status = 'ACTIVE'"
        self.cursor.execute(end_query)
        res=self.cursor.fetchall()
        today = date.today()
        for i in range(len(res)):
            if res[i][1] <= today:
                end_query="UPDATE eas_master SET status = 'COMPLETED' where uuid = '"+res[i][0]+"'"
                self.cursor.execute(end_query)
                CONNECTION.commit()

        for ele in range(len(result)):
            task_id = self.get_id(str(result[ele]))
            table_data = self.getdata(task_id)
            start_date=re.findall(r'\d+', str(table_data['start_date']))
            end_date=re.findall(r'\d+', str(table_data['end_date']))
            default_args = {'owner': 'airflow',
                    'provide_context':True,
                    'retries': 5,
                    'retry_delay': timedelta(minutes=1),
                    'start_date':datetime(int(start_date[0]),
                                           int(start_date[1]),
                                           int(start_date[2])), 
                    'end_date': datetime(int(end_date[0]),
                                           int(end_date[1]),
                                           int(end_date[2])),
                    }
            schedule = self.get_schedule(table_data['frequency'])
            self.create_dag(table_data['dag_name'], schedule, default_args, table_data)

dag_obj = STLDB2DAG()
dag_obj.retrieve_id()
CONNECTION.close()
