#
# Assignment5 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
import json
import math

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    f = open(saveLocation1, 'w')
    documents = collection.find({"city": { '$regex': cityToSearch, '$options': 'i'}},{'name':1, 'full_address': 1, 'city': 1, 'state': 1})
    for document in documents:
        name = document.get('name') 
        full_address = document.get('full_address')   
        city = document.get('city')
        state = document.get('state')
        f.write(name.upper() + '$' + full_address.upper() + '$' + city.upper() + '$' + state.upper() + '\n')

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    f = open(saveLocation2, 'w')
    documents = collection.find({"categories": { '$in': categoriesToSearch}}, {'name': 1, 'latitude':1, 'longitude': 1})
    for document in documents:
        name = document.get('name') 
        latitude = float(document.get('latitude')) 
        longitude = float(document.get('longitude')) 
        distance = DistanceFunction(float(myLocation[0]), float(myLocation[1]), latitude, longitude)
        if distance <= maxDistance:
            f.write(name.upper() + '\n')

def DistanceFunction(lat1, lon1, lat2, lon2):
    R = 3959; # miles
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2-lat1)
    delta_lambda = math.radians(lon2-lon1)
    
    a = math.sin(delta_phi/2) * math.sin(delta_phi/2) + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2) * math.sin(delta_lambda/2)
    
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = R * c
    return abs(d)