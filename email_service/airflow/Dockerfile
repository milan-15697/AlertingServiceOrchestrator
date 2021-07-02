FROM python:3.8

# supervisord setup
RUN apt-get update && apt-get install -y supervisor
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

COPY ./app/ /app/app
#Airflow setup
ENV AIRFLOW_HOME=/app/airflow

COPY requirements.txt $AIRFLOW_HOME/requirements.txt
RUN pip install --no-cache-dir -r $AIRFLOW_HOME/requirements.txt


COPY dags/ $AIRFLOW_HOME/dags/
RUN pip install  cattrs==1.0.0

COPY airflow.cfg /app/airflow/airflow.cfg

EXPOSE 8080

CMD ["/usr/bin/supervisord"]
