# Twitter-Elections :partying_face:
Prédiction des vainqueurs dans chaque circonscription aux élections législatives grâce à la popularité des tweets et leurs états civils.


## Deploy using Docker

#### Launch Docker to insert / update data
- cd docker/
- docker compose build
- docker compose up -d
- docker compose run python bash

Launch a python script:
- python 3 xxx.py

#### Use pgadmin interface 
The containeur has to run:
- cd docker/
- docker compose build
- docker compose up -d
- docker compose run python bash

- go to http://localhost:15433/
- use email and password as in env file
- Add a new serveur
- fill in the information with the env file

Retrieve IP Adress:
- docker ps: copy id of postgresql container
- docker inspect id_container | grep IPAddress

Have fun!