import pandas as pd
import numpy as np
from query1 import pretty_table
from neo4j_utilities import *
from postgres_utilities import *
from connections import *


# analyse the genre specific media score and user score
def query2():
    #connect to MongoDB,Neo4j,PostgreSQL
    collection,client=connect_mongodb()
    #driver=connect_neo4j()
    #session=driver.session()
    conn,cur=connect_postgres()
    
    #get game info from MongoDB
    json_games=collection.find({},{"_id":0,"sid":1,"gfq_rating":1,"meta_score":1,"meta_uscore":1,"igdb_score":1,"igdb_uscore":1,"igdb_popularity":1})
    json_games=pd.DataFrame(list(json_games))
    
    #insert to PostgreSQL
    insert_dataframe_to_postgres(json_games,"json_scores",cur,conn)
    
    # #inner join json score and steam based on sid
    # join_query="""
    # select s.appid,s.name,s.release_date,s.genres,j.gfq_rating,j.meta_score,
    # j.meta_uscore,j.igdb_score,j.igdb_uscore,j.igdb_popularity 
    # from steam as s inner join json_scores as j on j.sid=s.appid
    # """
    # cur.execute(join_query)
    
    groupby_query="""
    with t as (select s.appid,s.name,s.release_date,s.genres,j.gfq_rating,j.meta_score,
    j.meta_uscore,j.igdb_score,j.igdb_uscore,j.igdb_popularity 
    from steam as s inner join json_scores as j on j.sid=s.appid)
    SELECT 
    unnested_genres, 
    round(AVG(gfq_rating),2) AS avg_gfq_rating, 
    round(AVG(meta_score),2) AS avg_meta_score, 
    round(AVG(meta_uscore),2) AS avg_meta_uscore, 
    round(AVG(igdb_score),2) AS avg_igdb_score, 
    round(AVG(igdb_uscore),2) AS avg_igdb_uscore, 
    round(AVG(igdb_popularity),2) AS avg_igdb_popularity
    FROM 
    (SELECT 
            unnest(string_to_array(genres, ';')) AS unnested_genres, 
            gfq_rating, 
            meta_score, 
            meta_uscore, 
            igdb_score, 
            igdb_uscore, 
            igdb_popularity
        FROM 
            t)
    GROUP BY unnested_genres
    HAVING 
    AVG(gfq_rating) IS NOT NULL AND
    AVG(meta_score) IS NOT NULL AND
    AVG(meta_uscore) IS NOT NULL AND
    AVG(igdb_score) IS NOT NULL AND
    AVG(igdb_uscore) IS NOT NULL AND
    AVG(igdb_popularity) IS NOT NULL;
    """
    
    #execute query
    cur.execute(groupby_query)
    
    #get result
    result=cur.fetchall()
    column_names = [desc[0] for desc in cur.description]
    result=pd.DataFrame(result,columns=column_names)
    #print(result)
    #save to csv
    result.to_csv("result_query2.csv",index=False)
    
    
    #close connections
    cur.close()
    conn.close()
    client.close()
    #driver.close()
    


if __name__ == "__main__":
    query2()
