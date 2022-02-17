from confluent_kafka import Producer
from fastapi import FastAPI
from faustmodels import Person
import pandas as pd
from pydantic import BaseConfig, BaseModel
from typing import List, Union
import faust

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

import logging
logging.basicConfig(format=FORMAT, level=logging.DEBUG)
logger = logging.getLogger(__name__)


## Setup the app
app = FastAPI(debug=True)
BaseConfig.arbitrary_types_allowed = True

@app.get("/")
def root():
    return { "Hello" : "API"}

class FaustRecord(BaseModel):
    record: Union[dict, List[dict]]

## /Watch endpoint
watch_app = faust.App("put.watch", broker="redpanda:29092")
watch_topic = faust.Topic(watch_app, topics=["people-watching"],key_type=str, value_type=Person)

@app.post("/watch")
async def people_watch(msg: FaustRecord):
    t1 = pd.Timestamp("now")
    recs = []
    if not isinstance(msg.record, list):
        recs = [msg.record]
    else:
        recs = msg.record

    for rec in recs:
        await watch_topic.send(value=Person(**rec), key="name")
    return f"{len(recs)} people were seen in {abs(t1-pd.Timestamp('now')).total_seconds()}"
