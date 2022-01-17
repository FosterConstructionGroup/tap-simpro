import os
import json
import asyncio
import aiohttp
import singer
from singer import metadata

from tap_simpro.utility import (
    await_futures,
    get_abs_path,
    streams,
    sub_streams,
    format_date,
)
from tap_simpro.fetch import handle_resource

logger = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["access_token", "company_id"]


def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def populate_metadata(schema):
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), "table-key-properties", "ID")

    for field_name in schema["properties"].keys():
        mdata = metadata.write(
            mdata,
            ("properties", field_name),
            "inclusion",
            "automatic" if field_name == "ID" else "available",
        )

    return mdata


def get_catalog():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():
        # get metadata for each field
        mdata = populate_metadata(schema)

        # create and add catalog entry
        catalog_entry = {
            "stream": schema_name,
            "tap_stream_id": schema_name,
            "schema": schema,
            "metadata": metadata.to_list(mdata),
            "key_properties": "ID",
        }
        streams.append(catalog_entry)

    return {"streams": streams}


def do_discover():
    catalog = get_catalog()
    # dump catalog
    print(json.dumps(catalog, indent=2))


def get_selected_streams(catalog):
    """
    Gets selected streams.  Checks schema's 'selected'
    first -- and then checks metadata, looking for an empty
    breadcrumb and mdata with a 'selected' entry
    """
    selected_streams = []
    for stream in catalog["streams"]:
        stream_metadata = stream["metadata"]
        if stream["schema"].get("selected", False):
            selected_streams.append(stream["tap_stream_id"])
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry["breadcrumb"] and entry["metadata"].get("selected", None):
                    selected_streams.append(stream["tap_stream_id"])

    return selected_streams


def get_stream_from_catalog(stream_id, catalog):
    for stream in catalog["streams"]:
        if stream["tap_stream_id"] == stream_id:
            return stream
    return None


async def do_sync(session, state, catalog):
    selected_stream_ids = get_selected_streams(catalog)

    singer.write_state(state)

    stream_futures = []

    for stream in catalog["streams"]:
        stream_id = stream["tap_stream_id"]
        stream_schema = stream["schema"]
        mdata = stream["metadata"]

        schemas = {stream_id: stream_schema}

        # if stream is selected, write schema and sync
        if stream_id in selected_stream_ids and stream_id not in sub_streams:
            singer.write_schema(stream_id, stream_schema, stream["key_properties"])

            for substream_id in streams.get(stream_id, []):
                if substream_id in selected_stream_ids:
                    substream = get_stream_from_catalog(substream_id, catalog)
                    schemas[substream_id] = substream["schema"]
                    singer.write_schema(
                        substream_id, substream["schema"], substream["key_properties"]
                    )

            stream_futures.append(
                handle_resource(session, stream_id, schemas, state, mdata)
            )

    bookmarks_dicts = await await_futures(stream_futures)
    state = {k: format_date(v) for dict in bookmarks_dicts for k, v in dict.items()}
    singer.write_state(state)


async def run_async(config, state, catalog):
    access_token = config["access_token"]
    headers = {"Authorization": f"Bearer {access_token}"}

    async with aiohttp.ClientSession(headers=headers) as session:
        await do_sync(session, state, catalog)


@singer.utils.handle_top_exception(logger)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    if args.discover:
        do_discover()
    else:
        catalog = args.properties if args.properties else get_catalog()
        asyncio.get_event_loop().run_until_complete(
            run_async(args.config, args.state, catalog)
        )


if __name__ == "__main__":
    main()
