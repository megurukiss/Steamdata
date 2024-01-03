import psycopg2
from collections import defaultdict
import pandas as pd
import numpy as np

#process input name for ilike
def format_name_for_ilike(name):
    # Split the name into words and convert each to lowercase
    words = name.split()
    lowercase_words = [word.lower() for word in words]

    # Join the words with '%'
    formatted_name = '%'.join(lowercase_words)

    # Add '%' at the beginning and end for the ILIKE expression
    ilike_expression = f'%{formatted_name}%'
    return ilike_expression

#get single game id from name
def get_game_id_from_name(name,cur):
    name=format_name_for_ilike(name)
    cur.execute("select name,appid from steam where name ilike %s",(name,))
    result=[item for item in cur.fetchall()]
    return result

#insert dataFrame to PostgreSQL
def insert_dataframe_to_postgres(df,table_name,cur,conn):
    
    column_definitions = []
    for column in df.columns:
        # Get numpy dtype of column
        if df[column].dtype == 'int64':
            sql_dtype = 'INTEGER'
        elif df[column].dtype == 'float64':
            sql_dtype = 'numeric'
        elif df[column].dtype == 'object':
            sql_dtype = 'TEXT'  # assuming object dtype is used for strings
        else:
            sql_dtype = 'TEXT'  # default or throw error

        # Add formatted column definition
        column_definitions.append(f'"{column}" {sql_dtype}')
    
    
    #create table
    columns_sql = ', '.join(column_definitions)
    cur.execute(f"create table if not EXISTS {table_name} ({columns_sql});")
    
    # if table exists and is not empty, skip insertion
    cur.execute(f"SELECT EXISTS(SELECT 1 FROM {table_name} LIMIT 1);")
    if cur.fetchone()[0]:
        print("Table already exists and is not empty. Skipping insertion.")
        return
    
    #clear table
    #cur.execute(f"delete from {table_name}")
    
    #convert NaN to None
    df=df.replace(np.nan, None)
    
    placeholders = ', '.join(['%s'] * len(df.columns))
    insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
    
    #insert dataFrame to PostgreSQL
    for index,row in df.iterrows():
        row_data = tuple([None if pd.isna(value) else value for value in row])
        cur.execute(insert_query, row_data)
    print("inserted successfully")
    
    conn.commit()