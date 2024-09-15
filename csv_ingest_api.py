# install urllib3 library

import requests
import csv
import json
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from datetime import datetime


SPLUNK_TOKEN = '<provide splunk HEC token>'
URL="https://<hostname>:8088"  # splunk server URL
csv_file = "<path to the CSV file>"

JSON_HEADER_CONTENT_TYPE = {"Content-Type": "application/json"}

def get_csv_file():
    with open(csv_file, mode='r') as file:
        csv_reader = csv.DictReader(file)
        csv_data = [row for row in csv_reader]

    return csv_data

def build_payload(payload):
    payload = {
        "event": payload,  # Encapsulate the JSON data as an event
        "sourcetype": "_json",
     }

    return payload


def send_payload(payload, session):
    # Sending to Splunk HEC
    print((
        "Sending Payload with {} records.".format(len(payload))
    ))

    jsonStr = json.dumps(payload)


    try:
        res = session.post(URL + "/services/collector/event", jsonStr, timeout=20)
        print((
            "Sending {0}. Return Status_Code: {1} | "
            "Return Text: {2}".format(
                "Succeeded" if res.status_code == 200 else "Failed",
                res.status_code,
                res.text,
                print (res.text)
            )
        ))
        return res
    except (requests.ConnectionError, requests.Timeout) as e:
        print((
            "Sending failed with {}. Moving On..".format(e)
        ))



def main_function():

    payload = build_payload(get_csv_file())

    splunk_token = SPLUNK_TOKEN
    session = requests.Session()
    session.headers.update({"Authorization": splunk_token})
    session.headers.update(JSON_HEADER_CONTENT_TYPE)
    session.verify = False

    result_status = send_payload(payload, session)

    return result_status



def lambda_handler(event, context):


    try:

        ############### current time before start################################
        start_time = datetime.now()
        print("Script start time: ", (start_time))

        main_function()

        ############### script end time ########################

        end_time = datetime.now()
        print("\nScript end time: ", (end_time))

        elapsed_time = end_time - start_time
        print('\nExecution time {0}'.format(elapsed_time))


    except Exception as e:

        status = {'code': 500, 'msg': str(e)}


        resp = "function execution failed"

        raise (e)


if __name__ == '__main__':
    lambda_handler({'body': 'hello'}, 'context')


