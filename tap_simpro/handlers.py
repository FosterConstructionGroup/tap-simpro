from datetime import datetime, timezone
from tap_simpro.utility import (
    write_record,
    write_many,
    get_basic,
    await_futures,
)


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

    for job in rows:
        id = job["ID"]
        sections_futures.append(get_basic(session, resource, f"jobs/{id}/sections/"))

    sections = [
        section for jobs in await await_futures(sections_futures) for section in jobs
    ]

        for s in sections:
            s["JobID"] = id
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

    write_many(cost_centers, resource, schema, mdata, extraction_time)


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


handlers = {
    "customer_sites": handle_customer_sites,
    "schedules_blocks": handle_schedules_blocks,
    "job_sections": handle_job_sections,
    # this is really a sub-stream to job_sections so can't be called directly
    "job_cost_centers": None,
    "invoice_jobs": handle_invoice_jobs,
}
