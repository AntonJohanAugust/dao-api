from fastapi import FastAPI, Query, status
from fastapi.responses import JSONResponse
from marshmallow import Schema, fields, ValidationError

import api_core.db as db
from api_core.db import Database


db.init()
app = FastAPI()

database = Database()


class EventSchema(Schema):
    user_id = fields.Str(required=True)
    account_type = fields.Str(required=True)
    date = fields.Date(required=True)
    event_type = fields.Str(required=True)
    order_value = fields.Float(required=True, allow_none=True)
    version = fields.Str(required=True)


@app.post("/receive_event", status_code=status.HTTP_201_CREATED)
def receive_event(body: dict):
    try:
        event_data = EventSchema().load(body)
    except ValidationError as e:
        print(f"status: 422\ndata: {body}\nerror: {e}")
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"error": e.messages}
        )
    try:
        database.event.add(event_data)
    except Exception as e:
        print(f"status: 500\ndata: {body}\nerror: {e}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": e.messages}
        )


@app.get("/monitor", status_code=status.HTTP_200_OK)
def monitor(
    country: str = Query(None),
):
    try:
        return database.get_events_grouped(country=country)
    except Exception as e:
        print(f"status: 500\nerror: {e}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": e.messages},
        )
