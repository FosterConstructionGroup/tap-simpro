import os
import json
import hashlib
import asyncio
import singer
from singer import metadata
import singer.metrics as metrics
from datetime import datetime

from tap_simpro.config import streams, has_details


# constants
base_url = "https://fosters.simprosuite.com/api/v1.0/companies/0"
strip_href_url = "/api/v1.0/companies/0/"

sub_streams = set([x for v in streams.values() for x in v])

# no rate limiting or concurrent request limit mentioned in docs https://developer.simprogroup.com/apidoc/
sem = asyncio.Semaphore(32)


def get_endpoint(resource):
    return {
        "accounts": "setup/accounts/chartOfAccounts",
        "activities": "setup/activities",
        "cost_centers": "setup/accounts/costCenters",
        "payable_invoices": "accounts/payable/invoices",
        "project_status_codes": "setup/statusCodes/projects",
        "schedule_rates": "setup/labor/scheduleRates",
    }.get(resource, to_camel_case(resource))


async def get_resource(session, resource, bookmark):
    ls = []
    page = 1

    while True:
        json = await get_basic(
            session,
            resource,
            f"{get_endpoint(resource)}/?pageSize=250&page={page}&orderby=-DateModified",
        )

        fetch_details = has_details.get(resource, True)

        if len(json) == 0:
            break
        page += 1

        if fetch_details:
            details_futures = []
            for row in json:
                # use _href property if available, or use the default of resource plus ID
                details_url = (
                    f"{get_endpoint(resource)}/{row['ID']}"
                    if "_href" not in row
                    else (row["_href"].replace(strip_href_url, ""))
                )
                details_futures.append(get_basic(session, resource, details_url))
            details_ls = await await_futures(details_futures)

            for d in details_ls:
                # note that simple string comparison sorting works here, thanks to the date formatting
                if bookmark and d["DateModified"] < bookmark:
                    break

                ls.append(d)
        else:
            ls += json

    return ls


async def get_basic(session, resource, url):
    async with sem:
        with metrics.http_request_timer(resource) as timer:
            async with await session.get(f"{base_url}/{url}") as resp:
                timer.tags[metrics.Tag.http_status_code] = resp.status
                resp.raise_for_status()
                return await resp.json()


def transform_record(record, properties, json_encoded_columns):
    if "CustomFields" in record:
        map = {}
        for field in record["CustomFields"]:
            map[field["CustomField"]["Name"]] = field["Value"]
        record["CustomFields"] = map

    for col in json_encoded_columns:
        record[col] = json.dumps(record[col])

    return record


async def await_futures(futures):
    return await asyncio.gather(*futures)


date_format = "%Y-%m-%d"
datetime_format = "%Y-%m-%d %H:%M:%S"


def format_date(dt, format=datetime_format):
    return datetime.strftime(dt, format)


def parse_date(dt, format=date_format):
    return datetime.strptime(dt, format)


def try_parse_date(s, parse_format=date_format):
    try:
        return format_date(parse_date(s, parse_format), date_format)
    except:
        return None


def hash(s):
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def write_record(row, resource, schema, mdata, dt):
    with singer.Transformer() as transformer:
        rec = transformer.transform(row, schema, metadata=metadata.to_map(mdata))
    singer.write_record(resource, rec, time_extracted=dt)


def write_many(rows, resource, schema, mdata, dt):
    with metrics.record_counter(resource) as counter:
        for row in rows:
            write_record(row, resource, schema, mdata, dt)
            counter.increment()


# per https://stackoverflow.com/questions/19053707/converting-snake-case-to-lower-camel-case-lowercamelcase#19053800
def to_camel_case(snake_str):
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])
