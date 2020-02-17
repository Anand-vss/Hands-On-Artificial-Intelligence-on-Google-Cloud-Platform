# Importing required Dependencies.
import sqlalchemy
from google.cloud import pubsub_v1
import pandas as pd
import JSON
from google.cloud import storage
from sqlalchemy.sql import text as sa_text


def validate_aip(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    #Setting bucket and file name.
    bucket = event['bucket']
    file_path = event['name']

    #Code block to read file from GCS and load data in JSON format.
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.get_blob(file_path)
    contents = blob.download_as_string()
    contents = contents.decode("utf-8")
    data = JSON.loads(contents)

    #Reading required text field from data.
    output = data['responses'][0]['fullTextAnnotation']['text']
    output = output[:-1]

    #Code setup to convert output data from JSON file to the required format                                 for loading into invoice table.

    keys = ['Company_Name:', 'Client_Name:', 'Client_Address:', 'SOW_Number:',                                                 'Project_ID:', 'Invoice_Number:', 'Invoice_Date:',
            'Billing_Period:', 'Bank_Account_Number:', 'Bank_Name:',                                             'Balance_Due:', 'Developer Rate Hours Subtotal']

    other_key = "Developer Rate Hours Subtotal"

    output_list = output.split('\n')
    output_dict = {}
    other_value_list = []

    for op in output_list:
        present = False
        for key in keys:
            if key in op:
                dict_key = key.replace(':', '')
                output_dict[dict_key] = op.replace(key, '')
                present = True
            if not present:
                other_value_list.append(op)
            
    output_dict[other_key] = other_value_list

    df_invoice = pd.DataFrame.from_dict(output_dict)

    df_invoice[['Developer', 'Rate', 'Hours', 'Subtotal']] =                             df_invoice['Developer Rate Hours Subtotal'].str.split(expand=True)

    df_invoice = df_invoice.drop(columns=['Developer Rate Hours Subtotal'])

    #Establishing connection with Cloud SQL.
    db_con = sqlalchemy.create_engine('MYSQL+pymysql://<db_user>:                                     <db_pass>@/<db_name>?                                                                  unix_socket=/cloudsql/<cloud_sql_instance_name>'
    )

    #Loading data into Cloud SQL, invoice table.
    db_con.execute(sa_text('truncate table invoice'))
    df_invoice.to_sql('invoice',db_con, if_exists='append',index=False)
