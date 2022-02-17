from faustmodels import Person

import faust
import logging
import pandas as pd
import random as rand

## Setup Logger
FORMAT = "%(levelname)s:%(message)s"
logging_config = {
        "version": 1, # mandatory field
        # if you want to overwrite existing loggers' configs
        # "disable_existing_loggers": False,
        "formatters": {
            "basic": {
                "format": FORMAT,
            }
        },
        "handlers": {
            "console": {
                "formatter": "basic",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
                "level": "DEBUG",
            }
        },
        "loggers": {
            __name__: {
                "handlers": ["console"],
                "level": "DEBUG",
                # "propagate": False
            }
        },
    }

logging.basicConfig(format=FORMAT, level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = faust.App('MyFaustApp', broker="redpanda:29092")

people_topic = app.topic(
    "people-watching",
    key_type=str,
    value_type=Person
)

talking_topic = app.topic(
    "people-talking",
    key_type=str,
    value_type=Person
)

walking_topic = app.topic(
    "people-walking",
    key_type=str,
    value_type=Person
)

def people_talking(person: Person):
    #logger.info("processing Person")
    diff_person = person
    diff_person.talk()
    return diff_person

def people_walking(person: Person):
    #logger.info("processing Person")
    diff_person = person
    diff_person.walk()
    return diff_person

@app.task
async def peopletalker():
    async for person in people_topic.stream():
        #logger.info(f"Checked {person}")
        await talking_topic.send(value=people_talking(person), key=person["name"])

@app.task
async def peoplewalker():
    async for person in talking_topic.stream():
        #logger.info(f"Checked {person}")
        await walking_topic.send(value=people_walking(person), key=person["name"])

if __name__ == '__main__':
    app.main()