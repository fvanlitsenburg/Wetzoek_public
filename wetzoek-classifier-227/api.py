from typing import List, Optional
from fastapi import FastAPI
from pydantic import BaseModel
import logging
import os

from query_classifier.query_classifier import ModelProvider


logging.basicConfig(level=logging.DEBUG)
app = FastAPI()

mp = ModelProvider(
    os.environ.get('DUTCH_TXT_PATH', 'sample_data/dutch.txt'),
    os.environ.get('MODEL_PKL_PATH', 'sample_data/20230125model.pkl'),
    os.environ.get('LAW_TITLES_CSV_PATH', 'sample_data/20230206titles.csv')
    )


class SearchRequest(BaseModel):
    query: str


class SearchResponse(BaseModel):
    data: List[str] = None
    error: Optional[bool] = False
    error_message: Optional[str] = ""


class StatusResponse(BaseModel):
    status: str = 'up'


@app.post("/api/v1/classifier/search-query")
async def search(req: SearchRequest):
    predictions = mp.predict(req.query)
    logging.debug(predictions)
    if predictions:
        return {
            "data": predictions[0],
            "areaLevel1": predictions[1],
            "areaLevel2": predictions[2],
            "areaLevel3": predictions[3],
            "error": False,
            "error_message": ""
        }
    else:
        return {
            "data": [],
            "error": True,
            "error_message": "There was an error while searching for categories"
        }


@app.get("/api/v1/status")
async def status():
    return {"status": "up"}
