# Fake Data Generator

Simple Dockerized API that generates fake data according to received data and inserts into requested postgres database.
Program saves faked data in redis if user needs to get same data several times. Two tables are inserted into Postgres 
database first table named as "psa" + filename as an original data and table of faked data named as filename. 
Filename shouldn't contain names such as "password", "recovery" etc. to use query tool in Postgres. 
## Required Form of Request


API takes required inputs: 
- number - number as a string
- dataset - csv file
- conn - connection string as a stringify json


## Usage
- API takes number parameter and writes n times original length faked data. 
- API takes csv file, columns program can handle are:
Login email, Identifier, One-time password Recovery code, First name, Last name, Department, Location. If file contains other columns, column value will be None/null
- API takes connection string of a postgres database where data will be stored, connection string must be stringify json with following order: host, database, user, password, port.
```bash
# start postgres service
systemctl start postgresql

#start redis server
redis-server

# to build docker image
docker build .

# to run docker container and connect redis and postgres databases
 docker run --net=host [built_image_name]
```

