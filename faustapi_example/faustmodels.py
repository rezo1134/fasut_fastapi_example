from pydantic import BaseModel, create_model
from typing import List

import faust
import faker
import pandas as pd
import random as rand


class Person(faust.Record):
    name: str = None
    location: List[str] = None
    lat: float = None
    lon: float = None
    city: str = None
    country: str = None
    phrase: str = None

    def walk(self):
        choice = rand.choice([True, False])
        if choice == True:
            lat_choice = rand.choice(["pos_lat","neg_lat","None"])
            lon_choice = rand.choice(["pos_lon","neg_lon"])
            if lat_choice == "pos_lat":
                self.lat += round(rand.random()*.1,5)
            elif lat_choice == "neg_lat":
                self.lat -= round(rand.random()*.1,5)                

            if lon_choice == "pos_lon":
                self.lon += round(rand.random()*.1,5)
            elif lon_choice == "neg_lon":
                self.lon -= round(rand.random()*.1,5)

    def talk(self):
        choice = rand.choice([True, False])
        if choice == True:
            fakeit = faker.Faker()
            self.phrase = " ".join(fakeit.words(5))
            