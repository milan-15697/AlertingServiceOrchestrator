"""This file contains APIs for Email Alert Service."""
import os
import sys
import uuid
import json
from datetime import datetime
from datetime import date
from pydantic import BaseModel
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from psycopg2.extensions import AsIs
sys.path.append("../airflow/dags/")
from db_connector import DB_Connector

app = FastAPI()
try:
    CONNECTION = DB_Connector().connect_to_psql('eas')
except Exception as ex:
    raise HTTPException(status_code=404, detail=ex)

class PayloadIP(BaseModel):
    """Class to define the template of new request payload"""
    start_date: str
    end_date: str
    topic_layer: str
    email: str
    frequency: int

def convert_date(date):
    """Function to convert date into datetime variable"""
    try:
        date = datetime.strptime(date, '%Y-%m-%d')
    except Exception as ex:
        raise HTTPException(status_code=400, detail=str(ex))
    return date

def check_already_exists(cur, req_payload):
    """Function to check if request already exists in database"""
    req_payload = req_payload.replace("\\", "")

    query = f'''
                SELECT * from "eas_master" WHERE payload = \
                    '{req_payload}' AND status = 'ACTIVE'
            '''

    cur.execute(query)
    result = cur.fetchall()

    return True if len(result) == 0 else False

def insert_db(cur, payload):
    """Function to insert new request into the database"""
    if check_already_exists(cur, payload['payload']):
        columns = payload.keys()
        values = [payload[column] for column in columns]
        ins_que = 'insert into eas_master (%s) values %s Returning uuid'
        cur.execute(ins_que, (AsIs(','.join(columns)), tuple(values)))
        fetch_uuid = cur.fetchone()[0]
        payload['uuid'] = fetch_uuid
        if payload['status'] == 'ACTIVE':
            payload['message'] = 'New Subscription Registered Successfully'
        else:
            payload['message'] = 'Subscription limit reached for '+payload['topic_layer'].upper()
        del payload['payload']
        return payload
    else:
        raise HTTPException(status_code=400, detail="Duplicate Request")

@app.get('/eas_subscription/table')
async def get_table():
    """API to fetch the details of Active Subscriptions"""
    result = []
    cur = CONNECTION.cursor()
    cur.execute("SELECT uuid,email,dag_name,topic_layer,\
        frequency,start_date,end_date,status from eas_master")
    record = cur.fetchall()
    for row in record:
        final_dict = dict()
        final_dict['uuid'] = row[0]
        final_dict['email'] = row[1]
        final_dict['dag_name'] = row[2]
        final_dict['topic_layer'] = row[3]
        final_dict['frequency'] = row[4]
        final_dict['start_date'] = str(row[5])
        final_dict['end_date'] = str(row[6])
        final_dict['status'] = row[7]
        result.append(final_dict)

    if not record:
        return "No Active Subscriptions!!"
    else:
        return JSONResponse(result)

@app.post('/eas_subscription/create')
async def request_receiver(payload: PayloadIP):
    """API to register a new subscription for Email Alert Service"""
    cur = CONNECTION.cursor()
    payload = payload.dict()
    payload['payload'] = json.dumps(payload)
    start_date = convert_date(payload['start_date'])
    end_date = convert_date(payload['end_date'])
    today = convert_date(str(date.today()))

    if start_date < end_date and start_date >= today:
        lay = payload['topic_layer']
        check_que = "SELECT count(*) from eas_master where \
                    topic_layer= '"+lay+"' and status = 'ACTIVE'"
        cur.execute(check_que)
        record = cur.fetchall()
        record = record[0][0]

        if record < 5:
            payload['status'] = 'ACTIVE'
            dag_name = f"STL_{payload['topic_layer']}_{str(uuid.uuid4())[:5]}"
            payload['dag_name'] = dag_name.upper()
            response = insert_db(cur, payload)
        else:
            payload['status'] = 'FAILED'
            response = insert_db(cur, payload)

        CONNECTION.commit()
        return JSONResponse(response)
    else:
        raise HTTPException(status_code=400, \
            detail="Start Date should be less than End Date and should not be in the past")

@app.delete('/eas_subscription/delete')
async def delete_entry(uid):
    """API to delete the existing request and its corresponding DAG"""
    cur = CONNECTION.cursor()
    uid = uid.strip()
    fetch = "Select dag_name from eas_master where uuid='"+uid+"'"
    cur.execute(fetch)
    to_del = cur.fetchall()

    if to_del:
        del_query = "DELETE from eas_master where uuid='"+uid+"'"
        dag_del_command = f"airflow delete_dag {to_del[0][0]} -y"
        cur.execute(del_query)
        CONNECTION.commit()
        os.system(dag_del_command)
        return JSONResponse("Successfully Deleted "+to_del[0][0])
    else:
        response = "DAG corresponding to "+ uid +" doesn't exist"
        raise HTTPException(status_code=404, detail=response)
