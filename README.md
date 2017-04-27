# Filmoji
This is a simple parser, that uses https://www.themoviedb.org/ API to insert data to Neo4J database.

### Prerequisites

Install python 2.+ 
```
https://www.python.org/downloads/
```
Install neo4j
```
https://neo4j.com/download/
```
Install requests
```
$ pip install requests
```
Install py2neo
```
$ pip install py2neo
```
Create a developer account on https://www.themoviedb.org/

Create configuration.py file with content as below:
```
def get_db_username():
    return "your_username"


def get_db_password():
    return "your_password"


def get_db_addres():
    return "your_database_address" # for example "http://localhost:7474"


def get_api_key():
    return "?api_key=YOUR_API_KEY" # api key returned by themoviedb.org
