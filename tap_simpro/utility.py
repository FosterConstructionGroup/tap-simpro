import os
import requests
import singer
from singer import metadata
import singer.metrics as metrics
from datetime import datetime

from tap_simpro.config import streams

session = requests.Session()


# constants
baseUrl = "https://fosters.simprosuite.com/api/v1.0/companies/0"

sub_streams = set([x for v in streams.values() for x in v])


def get_endpoint(resource):
    if resource == "customers":
        return "customers/companies"
    else:
        return resource


def get_resource(resource, bookmark):
    with metrics.http_request_timer(resource) as timer:
        session.headers.update()

        ls = []
        page = 1

        while True:
            json = get_basic(
                resource,
                f"{get_endpoint(resource)}/?page_size=250&page={page}&orderby=-DateModified",
            )

            for row in json:
                id = row["ID"]
                details = get_basic(resource, f"{get_endpoint(resource)}/{id}")

                # note that simple string comparison sorting works here, thanks to the date formatting
                if bookmark and details["DateModified"] < bookmark:
                    break

                ls.append(details)

            if len(json) > 0:
                page += 1
            else:
                break

        return ls


def get_basic(resource, url):
    with metrics.http_request_timer(resource) as timer:
        resp = session.request(method="get", url=f"{baseUrl}/{url}")
        resp.raise_for_status()

        timer.tags[metrics.Tag.http_status_code] = resp.status_code

        return resp.json()


def transform_record(record, properties):
    if "CustomFields" in record:
        map = {}
        for field in record["CustomFields"]:
            map[field["CustomField"]["Name"]] = field["Value"]
        record["CustomFields"] = map

    return record


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


def write_bookmark(state, resource, dt):
    singer.write_bookmark(state, resource, "since", format_date(dt))
    return state
