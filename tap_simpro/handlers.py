from datetime import datetime, timezone
import re
from singer.bookmarks import get_bookmark

from tap_simpro.utility import write_record, write_many, get_basic, await_futures, hash


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


async def handle_job_sections(session, rows, schemas, state, mdata):
    resource = "job_sections"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    new_bookmarks = {resource: extraction_time}

    sections_futures = []
    cost_centers_futures = []

    # need this wrapper to return job ID, as otherwise there's no way to pass it through
    async def get(id):
        return (id, await get_basic(session, resource, f"jobs/{id}/sections/"))

    for job in rows:
        id = job["ID"]
        sections_futures.append(get(id))

    sections = [
        (job_id, section)
        for (job_id, job_sections) in await await_futures(sections_futures)
        for section in job_sections
    ]

    for (job_id, s) in sections:
        s["JobID"] = job_id
        write_record(s, resource, schema, mdata, extraction_time)

        if "job_cost_centers" in schemas:
            cost_centers_futures.append(
                handle_job_cost_centers(session, s, schemas, state, mdata)
            )
            new_bookmarks["job_cost_centers"] = extraction_time

    await await_futures(cost_centers_futures)
    return new_bookmarks


async def handle_job_cost_centers(session, section, schemas, state, mdata):
    resource = "job_cost_centers"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    job_id = section["JobID"]
    section_id = section["ID"]

    cost_centers = await get_basic(
        session, resource, f"jobs/{job_id}/sections/{section_id}/costCenters/"
    )

    for c in cost_centers:
        c["SectionID"] = section_id
        write_record(c, resource, schema, mdata, extraction_time)


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


handlers = {
    "contractor_timesheets": handle_contractor_timesheets,
    "customer_sites": handle_customer_sites,
    "employee_timesheets": handle_employee_timesheets,
    "invoice_jobs": handle_invoice_jobs,
    "job_sections": handle_job_sections,
    # this is really a sub-stream to job_sections so can't be called directly
    "job_cost_centers": None,
    "payable_invoices_cost_centers": handle_payable_invoices_cost_centers,
    "schedules_blocks": handle_schedules_blocks,
    "quote_sections": handle_quote_sections_cost_centers,
    # this is really a sub-stream to quote_sections so can't be called directly
    "quote_cost_centers": None,
}
