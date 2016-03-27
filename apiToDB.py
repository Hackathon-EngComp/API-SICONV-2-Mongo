# -*- coding: utf-8 -*-

import json
import requests
from pymongo import MongoClient

offset = 0
convenios = []
contConvenios = 0

databaseName = 'dadosSiconv'
collectionName = 'conveniosTeste'
mongoPort = 27017

client = MongoClient('localhost', mongoPort)
db = client[databaseName]
collection = db[collectionName]

request = requests.get('http://api.convenios.gov.br/siconv/v1/consulta/convenios.json')
dadosConvenios = request.json()

totalConvenios = dadosConvenios['metadados']['total_registros']
convenios = dadosConvenios['convenios']

while offset < totalConvenios:
    offset += 500
    contConvenios += 500
    request = requests.get("http://api.convenios.gov.br/siconv/v1/consulta/convenios.json?offset=%d" % offset)
    if request is None:
        break
    dadosConvenios = request.json()
    convenios += dadosConvenios['convenios']
    if contConvenios == 10000:  #limitações do mongo
        collection.insert_many(convenios)
        del convenios[:]
        contConvenios = 0
        print("Dados inseridos")
    print("%d de %d" % (offset, totalConvenios))

queryResult = collection.insert_many(convenios)
print("Total de registros: ")
print(collection.count())

