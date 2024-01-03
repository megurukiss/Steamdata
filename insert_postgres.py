import psycopg2
import csv

if __name__=="__main__":
    
    #connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            dbname="final",
            user="postgres",
            password="",
            host="localhost"
        )
        print("Connected to the database successfully")
    # Your database operations go here
    except Exception as e:
        print("An error occurred:", e)
    cur=conn.cursor()
    
    #create table
    path='./dataset/steam.csv'
    cur.execute("create table steam (appid int, name text, release_date varchar(100), english boolean, developer text, publisher text, platforms varchar(100), required_age int, categories text, genres text, steamspy_tags text, achievements int, positive_ratings int, negative_ratings int, average_playtime int, median_playtime int, owners varchar(100), price float);")
    conn.commit()
    
    #insert data
    with open(path) as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            cur.execute("insert into steam (appid, name, release_date, english, developer, publisher, platforms, required_age, categories, genres, steamspy_tags, achievements, positive_ratings, negative_ratings, average_playtime, median_playtime, owners, price) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s, %s, %s)",row)
            conn.commit()
    
    #close connection
    cur.close()
    conn.close()