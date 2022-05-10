from datetime import datetime, timezone
import re
from singer.bookmarks import get_bookmark
from aiohttp import ClientResponseError

from tap_simpro.utility import (
    write_record,
    write_many,
    get_basic,
    await_futures,
    hash,
    get_resource,
)


async def handle_contractor_timesheets(session, contractors, schemas, state, mdata):
    resource = "contractor_timesheets"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    futures = []

    for c in contractors:
        id = c["ID"]
        url = f"contractors/{id}/timesheets/?Includes=Job,Activity"

        futures.append(
            handle_timesheets(
                session, resource, id, url, schema, state, mdata, extraction_time
            )
        )

    await await_futures(futures)

    return {resource: extraction_time}


async def handle_customer_sites(session, rows, schemas, state, mdata):
    resource = "customer_sites"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    for row in rows:
        for site in row["Sites"]:
            record = {
                "ID": row["ID"] + site["ID"],
                "CustomerID": row["ID"],
                "SiteID": site["ID"],
            }
            write_record(record, resource, schema, mdata, extraction_time)

    return {resource: extraction_time}


async def handle_employee_timesheets(session, employees, schemas, state, mdata):
    resource = "employee_timesheets"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    futures = []

    for e in employees:
        id = e["ID"]
        url = f"employees/{id}/timesheets/?Includes=Job,Activity"

        futures.append(
            handle_timesheets(
                session, resource, id, url, schema, state, mdata, extraction_time
            )
        )

    await await_futures(futures)

    return {resource: extraction_time}


async def handle_invoice_jobs(session, invoices, schemas, state, mdata):
    resource = "invoice_jobs"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    ls = []

    for invoice in invoices:
        invoice_id = invoice["ID"]
        jobs = invoice["Jobs"]

        for j in jobs:
            j["InvoiceID"] = invoice_id
            # rename row ID to JobID so it's clearer
            j["JobID"] = j["ID"]
            j["ID"] = str(j["InvoiceID"]) + "_" + str(j["JobID"])

        ls += jobs

    write_many(ls, resource, schema, mdata, extraction_time)
    return {resource: extraction_time}


async def handle_job_tags(session, jobs, schemas, state, mdata):
    resource = "job_tags"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    for job in jobs:
        for tag in job.get("Tags", []):
            tag["JobID"] = job["ID"]
            tag["TagID"] = tag["ID"]
            tag["ID"] = str(tag["JobID"]) + "_" + str(tag["TagID"])

            write_record(tag, resource, schema, mdata, extraction_time)
    return {resource: extraction_time}


async def handle_job_sections_cost_centers(session, rows, schemas, state, mdata):
    s_resource = "job_sections"
    s_schema = schemas[s_resource]
    c_resource = "job_cost_centers"
    c_schema = schemas.get(c_resource)

    items_handlers = {
        "job_cost_center_catalog_item": "catalogs",
        "job_cost_center_labor_item": "labor",
        "job_cost_center_one_off_item": "oneOffs",
        "job_cost_center_prebuild_item": "prebuilds",
        "job_cost_center_service_fee": "serviceFees",
    }

    extraction_time = datetime.now(timezone.utc).astimezone()

    new_bookmarks = {s_resource: extraction_time}

    # far better parallelism getting everything at once than using `await` at the cost center granularity
    items_futures = []

    for job in rows:
        for s in job["Sections"]:
            s["JobID"] = job["ID"]
            write_record(s, s_resource, s_schema, mdata, extraction_time)

            if c_resource in schemas:
                new_bookmarks[c_resource] = extraction_time
                for c in s["CostCenters"]:
                    c["JobID"] = job["ID"]
                    c["SectionID"] = s["ID"]
                    write_record(c, c_resource, c_schema, mdata, extraction_time)

                    for (stream, suffix) in items_handlers.items():
                        if stream in schemas:
                            path_vars = {
                                "job_id": job["ID"],
                                "section_id": s["ID"],
                                "cost_center_id": c["ID"],
                            }
                            items_futures.append(
                                handle_job_cost_center_item(
                                    stream,
                                    session,
                                    path_vars,
                                    suffix,
                                    schemas[stream],
                                    get_bookmark(state, stream, "since"),
                                    mdata,
                                    extraction_time,
                                )
                            )
                            new_bookmarks[stream] = extraction_time

    await await_futures(items_futures)

    return new_bookmarks


async def handle_job_cost_center_item(
    resource,
    session,
    path_vars,
    endpoint_suffix,
    schema,
    bookmark,
    mdata,
    extraction_time,
):
    endpoint = f'jobs/{path_vars["job_id"]}/sections/{path_vars["section_id"]}/costCenters/{path_vars["cost_center_id"]}/{endpoint_suffix}'
    try:
        base_rows = await get_resource(
            session, resource, bookmark, endpoint_override=endpoint
        )
        rows = [{**row, **path_vars} for row in base_rows]
        write_many(rows, resource, schema, mdata, extraction_time)
    # service fees can throw a 404 instead of just returning [], so handle that case
    except ClientResponseError as e:
        if e.status == 404:
            pass
        else:
            raise e


async def handle_payable_invoices_cost_centers(
    session, invoices, schemas, state, mdata
):
    resource = "payable_invoices_cost_centers"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    for invoice in invoices:
        for cc in invoice["CostCenters"]:
            cc["ID"] = hash(
                "_".join(
                    [
                        invoice["OrderID"],
                        invoice["JobNo"],
                        invoice["AccountNo"],
                        invoice["Name"],
                    ]
                )
            )
            cc["OrderID"] = invoice["OrderID"]
            write_record(cc, resource, schema, mdata, extraction_time)

    return {resource: extraction_time}


async def handle_quote_sections_cost_centers(session, rows, schemas, state, mdata):
    s_resource = "quote_sections"
    s_schema = schemas[s_resource]
    c_resource = "quote_cost_centers"
    c_schema = schemas.get(c_resource)

    extraction_time = datetime.now(timezone.utc).astimezone()

    new_bookmarks = {s_resource: extraction_time}

    for quote in rows:
        for s in quote["Sections"]:
            s["QuoteID"] = quote["ID"]
            write_record(s, s_resource, s_schema, mdata, extraction_time)

            if c_resource in schemas:
                new_bookmarks[c_resource] = extraction_time
                for c in s["CostCenters"]:
                    c["QuoteID"] = quote["ID"]
                    c["SectionID"] = s["ID"]
                    write_record(c, c_resource, c_schema, mdata, extraction_time)

    return new_bookmarks


async def handle_schedules_blocks(session, rows, schemas, state, mdata):
    resource = "schedules_blocks"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    for row in rows:
        i = 0
        for block in row["Blocks"]:
            i += 1
            id = row["ID"]
            block["ID"] = f"{id}_{i}"
            block["ScheduleID"] = id
            write_record(block, resource, schema, mdata, extraction_time)

    return {resource: extraction_time}


async def handle_timesheets(
    session, resource, id, url, schema, state, mdata, extraction_time
):
    bookmark = get_bookmark(state, resource, "since")
    start_date = bookmark[:10] if bookmark else "2022-01-01"
    url = f"{url}&StartDate={start_date}"

    timesheets = await get_basic(session, resource, url)

    id_key = "EmployeeID" if resource == "employee_timesheets" else "ContractorID"
    id_prefix = "e" if resource == "employee_timesheets" else "c"

    for t in timesheets:
        t["ID"] = id_prefix + str(id) + "_" + t["Date"] + "_" + t["StartTime"]
        t[id_key] = id
        schedule_type = t["ScheduleType"]

        if schedule_type == "Job":
            reg = re.match(
                r"^/api/v1.0/companies/\d/jobs/(\d+)/sections/\d+/costCenters/(\d+)/schedules/(\d+)$",
                t["_href"],
            )
            t["JobID"] = reg[1]
            t["CostCenterID"] = reg[2]
            t["ScheduleID"] = reg[3]
        elif schedule_type == "Activity":
            reg = re.match(
                r"^/api/v1.0/companies/\d/activitySchedules/(\d+)$",
                t["_href"],
            )
            t["ActivityScheduleID"] = reg[1]

        write_record(t, resource, schema, mdata, extraction_time)


async def handle_vendor_order_item_allocations(
    session, vendor_orders, schemas, state, mdata
):
    resource = "vendor_order_item_allocations"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    async def handler(v):
        endpoint = f'vendorOrders/{v["ID"]}/catalogs'

        rows = await get_resource(
            session,
            resource,
            None,
            endpoint_override=endpoint,
            get_details_url=lambda row: f'{endpoint}/{row["Catalog"]["ID"]}',
        )

        for r in rows:
            for a in r["Allocations"]:
                a["VendorOrderID"] = v["ID"]
                a["CostCenterID"] = v.get("AssignedTo", {}).get("ID")
                a["Catalog"] = r["Catalog"]
                a["Price"] = r["Price"]
                a["ID"] = f'{v["ID"]}_{r["Catalog"]}'

        return rows

    base_rows = await await_futures([handler(v) for v in vendor_orders])
    flattened = [
        a
        for vendor_order in base_rows
        for row in vendor_order
        for a in row["Allocations"]
    ]
    write_many(flattened, resource, schema, mdata, extraction_time)

    return {resource: extraction_time}


handlers = {
    "contractor_timesheets": handle_contractor_timesheets,
    "customer_sites": handle_customer_sites,
    "employee_timesheets": handle_employee_timesheets,
    "invoice_jobs": handle_invoice_jobs,
    "job_tags": handle_job_tags,
    "job_sections": handle_job_sections_cost_centers,
    # job_cost_centers and children are sub-streams to job_sections so can't be called directly
    "payable_invoices_cost_centers": handle_payable_invoices_cost_centers,
    "schedules_blocks": handle_schedules_blocks,
    "quote_sections": handle_quote_sections_cost_centers,
    # quote_cost_centers is a sub-stream to quote_sections so can't be called directly
    "vendor_order_item_allocations": handle_vendor_order_item_allocations,
}
