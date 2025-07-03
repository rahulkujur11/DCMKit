from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from jsonschema import Draft7Validator
from kafka import KafkaProducer
from pathlib import Path
from starlette.status import (
    HTTP_200_OK, HTTP_201_CREATED, HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN, HTTP_405_METHOD_NOT_ALLOWED,
    HTTP_422_UNPROCESSABLE_ENTITY, HTTP_503_SERVICE_UNAVAILABLE
)
import json
import os

app = FastAPI()

BASE_DIR = Path(__file__).resolve().parent.parent

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def load_schema(schema_filename):
    schema_path = BASE_DIR / "api" / "schema" / schema_filename
    with open(schema_path) as f:
        return json.load(f)


def validate_and_process_payload(data, schema_filename, kafka_topic):
    schema = load_schema(schema_filename)
    validator = Draft7Validator(schema)

    if isinstance(data, dict):
        data = [data]
    elif not isinstance(data, list):
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST,
            detail="Payload must be a JSON object or list of objects"
        )

    valid_items = []
    errors = []

    for i, item in enumerate(data):
        errs = sorted(validator.iter_errors(item), key=lambda e: e.path)
        if errs:
            for e in errs:
                errors.append({
                    "index": i,
                    "field": list(e.path),
                    "message": e.message
                })
        else:
            valid_items.append(item)

    if errors:
        if valid_items:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail={"message": "Some items failed validation", "errors": errors}
            )
        else:
            raise HTTPException(
                status_code=HTTP_422_UNPROCESSABLE_ENTITY,
                detail={"message": "All items failed validation", "errors": errors}
            )

    for item in valid_items:
        producer.send(kafka_topic, value=item)
    producer.flush()

    return {
        "status": "sent to Kafka",
        "count": len(valid_items)
    }, (HTTP_201_CREATED if len(valid_items) == 1 else HTTP_200_OK)


@app.post("/comments")
async def receive_comment(request: Request):
    try:
        data = await request.json()
        result, status = validate_and_process_payload(
            data,
            "IdBasedComment-schema.json",
            "comments-topic"
        )
        return JSONResponse(status_code=status, content=result)
    except json.JSONDecodeError:
        return JSONResponse(
            status_code=HTTP_400_BAD_REQUEST,
            content={"detail": "Malformed JSON"}
        )
    except HTTPException as he:
        raise he
    except Exception as e:
        return JSONResponse(
            status_code=HTTP_503_SERVICE_UNAVAILABLE,
            content={"detail": f"Service unavailable: {str(e)}"}
        )


@app.post("/demand")
async def receive_demand(request: Request):
    try:
        data = await request.json()
        result, status = validate_and_process_payload(
            data,
            "WeekBasedMaterialDemand-schema.json",
            "demand-topic"
        )
        return JSONResponse(status_code=status, content=result)
    except json.JSONDecodeError:
        return JSONResponse(
            status_code=HTTP_400_BAD_REQUEST,
            content={"detail": "Malformed JSON"}
        )
    except HTTPException as he:
        raise he
    except Exception as e:
        return JSONResponse(
            status_code=HTTP_503_SERVICE_UNAVAILABLE,
            content={"detail": f"Service unavailable: {str(e)}"}
        )


@app.post("/capacity")
async def receive_capacity(request: Request):
    try:
        data = await request.json()
        result, status = validate_and_process_payload(
            data,
            "WeekBasedCapacityGroup-schema.json",
            "capacity-topic"
        )
        return JSONResponse(status_code=status, content=result)
    except json.JSONDecodeError:
        return JSONResponse(
            status_code=HTTP_400_BAD_REQUEST,
            content={"detail": "Malformed JSON"}
        )
    except HTTPException as he:
        raise he
    except Exception as e:
        return JSONResponse(
            status_code=HTTP_503_SERVICE_UNAVAILABLE,
            content={"detail": f"Service unavailable: {str(e)}"}
        )


# === Optional Global Error Handlers ===

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.exception_handler(401)
async def unauthorized_handler(request: Request, exc):
    return JSONResponse(status_code=HTTP_401_UNAUTHORIZED, content={"detail": "Unauthorized"})


@app.exception_handler(403)
async def forbidden_handler(request: Request, exc):
    return JSONResponse(status_code=HTTP_403_FORBIDDEN, content={"detail": "Forbidden"})


@app.exception_handler(405)
async def method_not_allowed_handler(request: Request, exc):
    return JSONResponse(status_code=HTTP_405_METHOD_NOT_ALLOWED, content={"detail": "Method Not Allowed"})


@app.exception_handler(503)
async def service_unavailable_handler(request: Request, exc):
    return JSONResponse(status_code=HTTP_503_SERVICE_UNAVAILABLE, content={"detail": "Service Unavailable"})
