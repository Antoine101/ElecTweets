from json_to_postgres import import_data_into_table_tweets

#/!\ A MODIFIER /!\
####### Chemin d'accès au fichier JSON de l'extraction des tweets d'un candidat######
json_file_path = 'PA721498__@StanGuerini__Guerini__Stanislas.json'                  #
#### ACCES DATABASE POSTGRES #####
user = 'postgres'
password = '123_nous_irons_aux_bois'
################################


import_data_into_table_tweets(json_file_path, password, user)