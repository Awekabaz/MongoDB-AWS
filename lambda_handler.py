import json
import pandas as pd
import boto3
from io import StringIO
import pymongo
import os
import utils.attributes
import formatColumns
from datetime import datetime


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

    # Getting the collections and converting to pandas dataframe for further processing
    df1 = pd.DataFrame(list(DataBase.collection1.find()))
    df2 = pd.DataFrame(list(DataBase.collection2.find()))
    df3 = pd.DataFrame(list(DataBase.collection3.find()))
    df4 = pd.DataFrame(list(DataBase.collection4.find()))

    now = datetime.now()
    OPERATION_DATE = now.strftime("%d-%m-%Y %H:%M:%S")


    # Selecting the needed attributes and dropping other
    df1 = df1[utils.attributes.toExport['df1']]
    df2 = df2[utils.attributes.toExport['df2']]
    df3 = df3[utils.attributes.toExport['df3']]
    df4 = df4[utils.attributes.toExport['df4']]

    # Formatting datetime columns: some has UNIX format some GMT
    formatColumns.formatDate(df1)
    formatColumns.formatDate(df2)
    formatColumns.formatDate(df3)
    formatColumns.formatDate(df4)

    # Format number columns
    formatColumns.formatNumber(df1)

    # Export the dataframes as .CSV, iterate over the dictionary
    # toCSV dictionary structure: list of [0] file name; [1] dataframe object
    toCSV = {'d1':['dataframe1.csv', df1], 'd2': ['dataframe2.csv', df2], 'd3':['dataframe3.csv', df3], 'd4':['dataframe4.csv', df4]}
    for structure in toCSV.values():
            fileNameCSV = structure[0]
            csv_buffer = StringIO()
            structure[1].to_csv(csv_buffer)
            
            # Upload the main files
            s3.put_object(
                Bucket = bucketName, 
                Key = fileNameCSV, 
                Body = csv_buffer.getvalue()
                )   
            
            # Archive the files
            s3.put_object(
                Bucket = bucketName, 
                Key = 'archive/' + OPERATION_DATE + '/' + fileNameCSV, 
                Body = csv_buffer.getvalue()
                )  
                
    infoString = 'd1: {}; d2: {}; d3: {}; d4: {};'.format(df1.shape,df2.shape,df3.shape,df4.shape)
    uploadByteStreams = bytes(json.dumps(infoString).encode('UTF-8'))
    uList = [('archive/' + OPERATION_DATE + '/' + 'infoString.json'), 'infoString.json']
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