from datetime import datetime, timezone
from singer.bookmarks import get_bookmark
from tap_simpro.utility import (
    get_resource,
    transform_record,
)
from tap_simpro.config import streams, json_encoded_columns, resource_details_url_fns
from tap_simpro.handlers import handlers
from tap_simpro.utility import write_many


async def handle_resource(session, resource, schemas, state, mdata):
    schema = schemas[resource]
    bookmark = get_bookmark(state, resource, "since")
    # Current time in local timezone as "aware datetime", per https://stackoverflow.com/a/25887393/7170445
    extraction_time = datetime.now(timezone.utc).astimezone()

    new_bookmark = {resource: extraction_time}

    rows = [
        transform_record(
            row, schema["properties"], json_encoded_columns.get(resource, [])
        )
        for row in await get_resource(
            session, resource, bookmark, resource_details_url_fns.get(resource)
        )
    ]

    for substream in streams.get(resource, []):
        if substream in schemas and handlers[substream] is not None:
            new_sub_bookmark = await handlers[substream](
                session, rows, schemas, state, mdata
            )
            new_bookmark = {**new_bookmark, **new_sub_bookmark}

    write_many(rows, resource, schema, mdata, extraction_time)
    return new_bookmark
