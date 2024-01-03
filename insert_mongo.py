from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json

if __name__ == "__main__":
    
    #connect to MongoDB
    uri = ""
    client=MongoClient(uri,server_api=ServerApi('1'))
    
    #check connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    
    #open dataset
    path='./dataset/steamdb.json'
    with open(path, 'r') as file:
        data = json.load(file)
    
    #insert data
    db=client['steam']
    collection=db['steamdb']
    collection.insert_many(data)
    
    #close connection
    client.close()