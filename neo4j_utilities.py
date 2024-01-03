from collections import defaultdict
import pandas as pd

#return dictionary of genres from name
def get_genres_from_name(lowerCaseName,session):
    n4j=session.run("match p=(g:Game)-[:IS_GENRE]->() where tolower(g.name) contains $name RETURN p",parameters={'name':lowerCaseName}).data()
    genres=defaultdict(list)
    for item in n4j:
        genres[(item['p'][0]['name'],item['p'][0]['id'])].append(item['p'][-1]['genre'])
    #genres=[item['p'][-1]['genre'] for item in n4j]
    return genres

#return list of genres from id
def get_genres_from_id(id,session):
    n4j=session.run("match p=(g:Game)-[:IS_GENRE]->(genre:Genre) where g.id=$id RETURN genre",parameters={'id':id}).data()
    genres=[item['genre']['genre'] for item in n4j]
    return genres

#return dataFrame of games from genres(list)
def get_games_from_genres(genres,session):
    query = """
        MATCH (g:Game)
        WHERE ALL(genre IN $genres WHERE (g)-[:IS_GENRE]->(:Genre {genre: genre}))
        RETURN g.name as Name,g.id as Appid
        """
    n4j=session.run(query,genres=genres).data()
    return pd.DataFrame(n4j)

# get game with a specific developer
def get_game_developer(session):
    query = """
        MATCH (g:Game)-[:DEVELOPED_BY]->(d:Developer)
        RETURN g.name as Name,toInteger(g.id) as appid,d.name as Developer
        """
    n4j=session.run(query).data()
    return pd.DataFrame(n4j)

    # if __name__ == "__main__":
    # from connections import *
    # #connect to Neo4j
    # driver=connect_neo4j()
    # session=driver.session()
    # #print(get_genres_from_name("dota",session))
    # #print(get_genres_from_id("570",session))
    # print(get_game_developer(session).head())
    # #print(get_game_developer(session))