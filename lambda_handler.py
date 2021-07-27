import json
import pandas as pd
import boto3
from io import StringIO
import os
import utils.attributes
import formatColumns
from datetime import datetime
from myContextManager import myConnectionManager

def lambda_handler(event, context):
    
    # Getting environment variables and initializing the clients
    dbURL = os.environ['SECURITY_STRING']
    bucketName = os.environ['BUCKET_NAME']
    s3 = boto3.client('s3')

    with myConnectionManager(dbURL) as mongo:
        DataBase = mongo.connection.get_database()
        # Getting the collections and converting to pandas dataframe for further processing
        df1 = pd.DataFrame(list(DataBase.collection1.find()))
        df2 = pd.DataFrame(list(DataBase.collection2.find()))
        df3 = pd.DataFrame(list(DataBase.collection3.find()))
        df4 = pd.DataFrame(list(DataBase.collection4.find()))

    # Get the operation date and time for further archiving
    myDatetime = datetime.now() + datetime.timedelta(hours=8)
    year = str(myDatetime.year)
    month = myDatetime.strftime("%B")
    OPERATION_DATE = myDatetime.strftime("%d-%m-%Y %H:%M:%S")

    # Selecting the needed attributes and dropping other
    df1 = df1[utils.attributes.toExport['df1']]
    df2 = df2[utils.attributes.toExport['df2']]
    df3 = df3[utils.attributes.toExport['df3']]
    df4 = df4[utils.attributes.toExport['df4']]

    toCSV = {'d1':df1, 'd2': df2, 'd3': df3, 'd4': df4}
    for df in toCSV.values():
        formatColumns.formatDate(df)
    
    # Formatting datetime columns: some has UNIX format some GMT
    formatColumns.formatDate(df1)
    formatColumns.formatDate(df2)
    formatColumns.formatDate(df3)
    formatColumns.formatDate(df4)

    # Format number columns
    formatColumns.formatNumber(df1)

    # Export the dataframes as .CSV, iterate over the dictionary
    # toCSV dictionary structure: list of [0] file name; [1] dataframe object
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

    # Create, upload and archive a JSON file with info about the operation finished      
    infoString = 'DATE: {}; d1: {}; d2: {}; d3: {}; d4: {};'.format(OPERATION_DATE, df1.shape,df2.shape,df3.shape,df4.shape)
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