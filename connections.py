from neo4j import GraphDatabase
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import psycopg2

def connect_mongodb():
    #connect to MongoDB
    uri = ""
    client=MongoClient(uri,server_api=ServerApi('1'))
    db=client['steam']
    collection=db['steamdb']
    return collection,client

def connect_neo4j():
    #connect to Neo4j
    URI=""
    USERNAME="neo4j"
    PASSWORD=""
    driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))
    driver.verify_connectivity()
    return driver

def connect_postgres():
    #connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            dbname="final",
            user="postgres",
            password="",
            host="localhost"
        )
        print("Connected to the postgres successfully")
    except Exception as e:
        print("An error occurred:", e)
    cur=conn.cursor()
    return conn,cur