from datetime import datetime, timezone
import singer
import singer.metrics as metrics
from singer import metadata
from singer.bookmarks import get_bookmark
from tap_simpro.utility import (
    get_resource,
    transform_record,
    parse_date,
    format_date,
    date_format,
    try_parse_date,
)


def handle_resource(resource, schemas, state, mdata):
    schema = schemas[resource]
    bookmark = get_bookmark(state, resource, "since")
    # Current time in local timezone as "aware datetime", per https://stackoverflow.com/a/25887393/7170445
    extraction_time = datetime.now(timezone.utc).astimezone()

    rows = [
        transform_record(row, schema["properties"])
        for row in get_resource(resource, bookmark)
    ]

    if resource == "customers":
        handle_customer_sites(rows, schemas, state, mdata)
    elif resource == "schedule":
        handle_schedule_blocks(rows, schemas, state, mdata)

    write_many(rows, resource, schema, mdata, extraction_time)
    return write_bookmark(state, resource, extraction_time)


def handle_customer_sites(rows, schemas, state, mdata):
    resource = "customer_sites"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    for row in rows:
        for site_id in row["Sites"]:
            record = {"CustomerID": row["ID"], "SiteID": site_id}
            write_record(record, resource, schema, mdata, extraction_time)

    write_bookmark(state, resource, extraction_time)


def handle_schedule_blocks(rows, schemas, state, mdata):
    resource = "schedule_blocks"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    for row in rows:
        for block in row["Blocks"]:
            block["ScheduleID"] = row["ID"]
            write_record(block, resource, schema, mdata, extraction_time)

    write_bookmark(state, resource, extraction_time)


def write_many(rows, resource, schema, mdata, dt):
    with metrics.record_counter(resource) as counter:
        for row in rows:
            write_record(row, resource, schema, mdata, dt)
            counter.increment()


def write_record(row, resource, schema, mdata, dt):
    with singer.Transformer() as transformer:
        rec = transformer.transform(row, schema, metadata=metadata.to_map(mdata))
    singer.write_record(resource, rec, time_extracted=dt)


def write_bookmark(state, resource, dt):
    singer.write_bookmark(state, resource, "since", format_date(dt))
    return state
