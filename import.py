import json

import requests
from py2neo import Graph, Node, Relationship, NodeSelector

import configuration as conf

API_KEY = conf.get_api_key()

PAGE_LINK = "https://api.themoviedb.org/3/"
MOVIE_LINK = "movie/"
PERSON_LINK = "person/"
CASTS_LINK = "/casts"
LATEST_LINK = "latest"

MOVIE_STR = "Movie"
ACTOR_STR = "Actor"
GENRE_STR = "Genre"
PROD_COMPANY_STR = "Production Company"

ACTED_STR = "ACTED_IN"
OF_STR = "OF"
RELEASED = "RELEASED"


def get_movie_link(index):
    return PAGE_LINK + MOVIE_LINK + str(index) + API_KEY


def get_latest_movie_id():
    url = PAGE_LINK + MOVIE_LINK + LATEST_LINK + API_KEY
    return json.loads(requests.get(url).text)["id"]


def get_cast_link(index):
    return PAGE_LINK + MOVIE_LINK + str(index) + CASTS_LINK + API_KEY


def get_person_link(index):
    return PAGE_LINK + PERSON_LINK + str(index) + API_KEY


def get_from_db_if_exists(key, index, db):
    selector = NodeSelector(db)
    selected = selector.select(key, id=index)
    if len(list(selected)) > 0:
        return list(selected)[0]
    return False


def get_movie_node(parsed_json, movie_id, db, transaction):
    if "status_code" in parsed_json:
        return False

    movie = get_from_db_if_exists(MOVIE_STR, movie_id, db)
    if not movie:
        movie = Node(MOVIE_STR, original_title=parsed_json["original_title"].encode('utf-8'),
                     budget=parsed_json["budget"], homepage=parsed_json["homepage"],
                     id=parsed_json["id"], imdb_id=parsed_json["imdb_id"],
                     original_language=parsed_json["original_language"], overview=parsed_json["overview"],
                     popularity=parsed_json["popularity"], status=parsed_json["status"],
                     tagline=parsed_json["tagline"], vote_average=parsed_json["vote_average"],
                     vote_count=int(parsed_json["vote_count"])
                     )
        print "inserting " + parsed_json["original_title"]
        transaction.create(movie)
    return movie


def add_genre_to_movie(index, name, movie, db, transaction):
    genre = get_from_db_if_exists(GENRE_STR, index, db)
    if not genre:
        genre = Node(GENRE_STR, id=index, name=name)
        transaction.create(genre)
    r = Relationship(movie, OF_STR, genre)
    if not db.exists(r):
        transaction.create(r)


def add_production_company_to_movie(index, name, movie, db, transaction):
    prod_company = get_from_db_if_exists(PROD_COMPANY_STR, index, db)
    if not prod_company:
        prod_company = Node(PROD_COMPANY_STR, id=index, name=name)
        transaction.create(prod_company)
    r = Relationship(prod_company, RELEASED, movie)
    if not db.exists(r):
        transaction.create(r)


def add_actor_to_movie(index, parsed_json_cast, movie, db, transaction):
    if "status_code" in parsed_json_cast:
        return False

    actor = get_from_db_if_exists(ACTOR_STR, index, db)
    if not actor:
        parsed_json_actor = json.loads(requests.get(get_person_link(index)).text)
        actor = Node(ACTOR_STR, biography=parsed_json_actor["biography"], birthday=parsed_json_actor["birthday"],
                     deathday=parsed_json_actor["deathday"], gender=parsed_json_actor["gender"],
                     id=parsed_json_actor["id"], imdb_id=parsed_json_actor["imdb_id"],
                     name=parsed_json_actor["name"].encode('utf-8'),
                     place_of_birth=parsed_json_actor["place_of_birth"],
                     popularity=parsed_json_actor["popularity"], )

        transaction.create(actor)

    r = Relationship(actor, ACTED_STR, movie, order=parsed_json_cast["order"], character=parsed_json_cast["character"])
    if not db.exists(r):
        transaction.create(r)


def add_crew_member_to_movie(index, parsed_json_crew, movie, db, transaction):
    if "status_code" in parsed_json_crew:
        return False

    person = get_from_db_if_exists(parsed_json_crew["job"], index, db)
    if not person:
        parsed_json_person = json.loads(requests.get(get_person_link(index)).text)
        person = Node(ACTOR_STR, biography=parsed_json_person["biography"], birthday=parsed_json_person["birthday"],
                      deathday=parsed_json_person["deathday"], gender=parsed_json_person["gender"],
                      id=parsed_json_person["id"], imdb_id=parsed_json_person["imdb_id"],
                      name=parsed_json_person["name"].encode('utf-8'),
                      place_of_birth=parsed_json_person["place_of_birth"],
                      popularity=parsed_json_person["popularity"], )

        transaction.create(person)

    r = Relationship(person, parsed_json_crew["department"], movie)
    if not db.exists(r):
        transaction.create(r)


def run():
    movie_id = 1
    db = Graph(conf.get_db_addres(), username=conf.get_db_username(),
               password=conf.get_db_password())

    latest_id = get_latest_movie_id()

    while movie_id < latest_id:
        parsed_json_cast = json.loads(requests.get(get_cast_link(movie_id)).text)
        parsed_json_movie = json.loads(requests.get(get_movie_link(movie_id)).text)
        transaction = db.begin()
        movie = get_movie_node(parsed_json_movie, movie_id, db, transaction)

        if movie is not False:
            for genre in parsed_json_movie["genres"]:
                add_genre_to_movie(genre["id"], genre["name"], movie, db, transaction)
            for comp in parsed_json_movie["production_companies"]:
                add_production_company_to_movie(comp["id"], comp["name"], movie, db, transaction)
            for actor in parsed_json_cast["cast"]:
                add_actor_to_movie(actor["id"], actor, movie, db, transaction)
            for person in parsed_json_cast["crew"]:
                add_crew_member_to_movie(person["id"], person, movie, db, transaction)
        transaction.commit()
        movie_id += 1


if __name__ == "__main__":
    run()
