from neo4j import GraphDatabase
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import psycopg2
from collections import defaultdict
import pandas as pd
from prettytable import PrettyTable
from neo4j_utilities import *
from postgres_utilities import *
from connections import *

#print table
def pretty_table(df):
    table = PrettyTable()
    table.field_names = df.columns
    for row in df.itertuples(index=False):
        table.add_row(row)
    # Print the table
    print(table)

#query the game with same genres
def query1():
    #connect to MongoDB
    collection,client=connect_mongodb()
    
    #connect to Neo4j
    driver=connect_neo4j()
    session=driver.session()
    
    #connect to PostgreSQL
    conn,cur=connect_postgres()
    #input name
    print("Please input the name of the game you want to search:")
    name=input()
    
    #select id from game name
    name_id=get_game_id_from_name(name,cur)
    
    #print games with same name
    print("-" * 50)
    print("Please choose the game you want to search and input a number:")
    print("-" * 50)
    for idx,(name,id) in enumerate(name_id):
        print(str(idx)+")"+name)
    
    #input number
    num=int(input())
    
    #return list of genres for input game
    name,id=name_id[num]
    genres=get_genres_from_id(str(id),session)
    
    
    #write similar games into result.txt
    with open("result_query1.txt","w") as f:
        games=get_games_from_genres(genres,session)
        f.write("-" * 50+"\n")
        f.write("\n"+"Similar games for "+name+" with genres "+str(genres)+"\n")
        f.write("-" * 50+"\n")
        f.write(games.to_string()+"\n")
        pretty_table(games)

    #close connections
    cur.close()
    conn.close()
    session.close()
    client.close()


if __name__=="__main__":
    query1()

"""
#first query: select similar games with same genres
if __name__=="__main__":
    
    #connect to MongoDB
    collection,client=connect_mongodb()
    
    #connect to Neo4j
    driver=connect_neo4j()
    session=driver.session()
    
    #connect to PostgreSQL
    conn,cur=connect_postgres()
    
    
    
    #input name
    print("Please input the name of the game you want to search:")
    name=input()
    
    #select id from game name
    name_id=get_game_id_from_name(name,cur)
    
    #print games with same name
    print("-" * 50)
    print("Please choose the game you want to search and input a number:")
    print("-" * 50)
    for idx,(name,id) in enumerate(name_id):
        print(str(idx)+")"+name)
    
    #input number
    num=int(input())
    
    #return list of genres for input game
    name,id=name_id[num]
    genres=get_genres_from_id(str(id),session)
    
    
    #write similar games into result.txt
    with open("result.txt","w") as f:
        games=get_games_from_genres(genres,session)
        f.write("-" * 50+"\n")
        f.write("\n"+"Similar games for "+name+" with genres "+str(genres)+"\n")
        f.write("-" * 50+"\n")
        f.write(games.to_string()+"\n")
        pretty_table(games)
          
    #close connections
    cur.close()
    conn.close()
    session.close()
    client.close()
"""