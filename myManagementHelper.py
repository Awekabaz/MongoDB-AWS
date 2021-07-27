import pymongo

class myConnectionManager():
    def __init__(self, connectionString):
        self.connectionURI = connectionString
        self.connection = None
  
    def __enter__(self):
        self.connection = pymongo.MongoClient(self.connectionURI, serverSelectionTimeoutMS=20000)
        try:
            print(self.connection.server_info())
        except Exception:
            print("Unable to connect to the server.")
            return{
            'Status': 667,
            'Uploaded': 'FAIL',
            'Collections uploaded': 'None'
            }
        return self
  
    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.connection.close()