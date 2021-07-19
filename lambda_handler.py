import json
import pandas as pd
import boto3
from io import StringIO
import pymongo
import os
import utils.attributes
import formatColumns
from datetime import datetime, timedelta


def lambda_handler(event, context):
    
    # Getting environment variables and initializing the clients
    dbURL = os.environ['SECURITY_STRING']
    bucketName = os.environ['BUCKET_NAME']
    client = pymongo.MongoClient(dbURL, serverSelectionTimeoutMS=20000)
    s3 = boto3.client('s3')
    DataBase = client.get_database()

    # Checking the connection
    try:
        print(client.server_info())
    except Exception:
        print("Unable to connect to the server.")
        return{
        'Status': 667,
        'Uploaded': 'FAIL',
        'Collections uploaded': 'None'
        }
    
    myDatetime = datetime.now() + timedelta(hours=8)
    year = str(myDatetime.year)
    month = myDatetime.strftime("%B")
    OPERATION_DATE = myDatetime.strftime("%d-%m-%Y %H:%M:%S")
    
    # Getting the collections and converting to pandas dataframe for further processing
    df_registers = pd.DataFrame(list(DataBase.registers.find()))
    df_users = pd.DataFrame(list(DataBase.users.find()))
    df_checkins = pd.DataFrame(list(DataBase.rankingcheckins.find()))
    df_courses = pd.DataFrame(list(DataBase.courses.find()))
    df_events = pd.DataFrame(list(DataBase.events.find()))

    


    # Selecting the needed attributes and dropping other
    df_users = df_users[utils.attributes.toExport['users']]
    df_registers = df_registers[utils.attributes.toExport['registers']]
    df_checkins = df_checkins[utils.attributes.toExport['checkins']]
    df_courses = df_courses[utils.attributes.toExport['courses']]
    df_events = df_events[utils.attributes.toExport['events']]

    # Formatting datetime columns: some has UNIX format some GMT
    formatColumns.formatDate(df_users)
    formatColumns.formatDate(df_registers)
    formatColumns.formatDate(df_courses)
    formatColumns.formatDate(df_checkins)
    formatColumns.formatDate(df_events)

    # Format number columns
    formatColumns.formatNumber(df_users)

    # Export the dataframes as .CSV, iterate over the dictionary
    # toCSV dictionary structure: list of [0] file name; [1] dataframe object
    toCSV = {'users':df_users, 'registers': df_registers, 'checkins': df_checkins, 'courses': df_courses, 'events': df_events}

    for structure in toCSV:
            fileNameCSV = structure + '.csv'
            csv_buffer = StringIO()
            toCSV[structure].to_csv(csv_buffer, index = False)
            
            # Upload the main files
            s3.put_object(
                Bucket = bucketName, 
                Key = fileNameCSV, 
                Body = csv_buffer.getvalue()
                )   
            
            # Archive the files
            s3.put_object(
                Bucket = bucketName, 
                Key = 'archive/' + year + '/' + month + '/' + OPERATION_DATE + '/' + fileNameCSV, 
                Body = csv_buffer.getvalue()
                )  
                
    infoString = 'DATE: {}; users: {}; registers: {}; checkins: {}; courses: {}; events: {};'.format(OPERATION_DATE,df_users.shape,df_registers.shape,df_checkins.shape,df_courses.shape, df_events.shape)
    uploadByteStreams = bytes(json.dumps(infoString).encode('UTF-8'))
    uList = [('archive/' + year + '/' + month + '/' + OPERATION_DATE + '/' + 'infoString.json'), 'infoString.json']
    for key in uList:
        s3.put_object(
                    Bucket = bucketName, 
                    Key = key, 
                    Body = uploadByteStreams
                    )
    return{
        'Status': 200,
        'Uploaded': 'SUCCESS',
        'Collections uploaded': infoString
    }