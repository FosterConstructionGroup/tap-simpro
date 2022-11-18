from datetime import datetime, timezone
from singer.bookmarks import get_bookmark
import singer.metrics as metrics
from tap_simpro.utility import (
    get_resource,
    transform_record,
)
from tap_simpro.config import streams, json_encoded_columns, resource_details_url_fns
from tap_simpro.handlers import handlers
from tap_simpro.transforms import transforms
from tap_simpro.utility import write_record


async def handle_resource(session, resource, schemas, state, mdata):
    schema = schemas[resource]
    bookmark = get_bookmark(state, resource, "since")
    # Current time in local timezone as "aware datetime", per https://stackoverflow.com/a/25887393/7170445
    extraction_time = datetime.now(timezone.utc).astimezone()

    substream_handlers = [
        handlers[substream]
        for substream in streams.get(resource, [])
        if substream in schemas and substream in handlers
    ]

    new_bookmark = {resource: extraction_time}
    for substream in streams.get(resource, []):
        new_bookmark[substream] = extraction_time

    with metrics.record_counter(resource) as counter:
        async for r in get_resource(
            session, resource, bookmark, schema, resource_details_url_fns.get(resource)
        ):
            row = transform_record(
                r, schema["properties"], json_encoded_columns.get(resource, [])
            )

            # only for top-level resources as sub-streams already have handler functions
            if resource in transforms:
                transforms[resource](row)

            write_record(row, resource, schema, mdata, extraction_time)
            counter.increment()

            for fn in substream_handlers:
                await fn(session, row, schemas, state, mdata)

    return new_bookmark
