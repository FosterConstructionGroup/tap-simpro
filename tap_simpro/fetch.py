from datetime import datetime, timezone
import singer.metrics as metrics
from singer.bookmarks import get_bookmark
from tap_simpro.utility import (
    get_resource,
    transform_record,
)
from tap_simpro.config import streams
from tap_simpro.handlers import handlers
from tap_simpro.utility import write_many, write_bookmark


def handle_resource(resource, schemas, state, mdata):
    schema = schemas[resource]
    bookmark = get_bookmark(state, resource, "since")
    # Current time in local timezone as "aware datetime", per https://stackoverflow.com/a/25887393/7170445
    extraction_time = datetime.now(timezone.utc).astimezone()

    rows = [
        transform_record(row, schema["properties"])
        for row in get_resource(resource, bookmark)
    ]

    for substream in streams.get(resource, []):
        if handlers[substream] is not None:
            handlers[substream](rows, schemas, state, mdata)

    write_many(rows, resource, schema, mdata, extraction_time)
    return write_bookmark(state, resource, extraction_time)
