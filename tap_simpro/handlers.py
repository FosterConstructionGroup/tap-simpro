from datetime import datetime, timezone
from tap_simpro.utility import (
    write_record,
    write_many,
    write_bookmark,
    get_basic,
)


def handle_customer_sites(rows, schemas, state, mdata):
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

    write_bookmark(state, resource, extraction_time)


def handle_schedules_blocks(rows, schemas, state, mdata):
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

    write_bookmark(state, resource, extraction_time)


def handle_job_sections(rows, schemas, state, mdata):
    resource = "job_sections"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    for job in rows:
        id = job["ID"]
        sections = get_basic(resource, f"jobs/{id}/sections/")

        for s in sections:
            s["JobID"] = id
            write_record(s, resource, schema, mdata, extraction_time)

            if "job_cost_centers" in schemas:
                handle_job_cost_centers(s, schemas, state, mdata)

    write_bookmark(state, resource, extraction_time)


def handle_job_cost_centers(section, schemas, state, mdata):
    resource = "job_cost_centers"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    job_id = section["JobID"]
    section_id = section["ID"]

    cost_centers = get_basic(
        resource, f"jobs/{job_id}/sections/{section_id}/costCenters/"
    )

    for c in cost_centers:
        c["SectionID"] = section_id

    write_many(cost_centers, resource, schema, mdata, extraction_time)
    write_bookmark(state, resource, extraction_time)


def handle_invoice_jobs(invoices, schemas, state, mdata):
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
            j["ID"] = None

        ls += jobs

    write_many(ls, resource, schema, mdata, extraction_time)
    write_bookmark(state, resource, extraction_time)


handlers = {
    "customer_sites": handle_customer_sites,
    "schedules_blocks": handle_schedules_blocks,
    "job_sections": handle_job_sections,
    # this is really a sub-stream to job_sections so can't be called directly
    "job_cost_centers": None,
    "invoice_jobs": handle_invoice_jobs,
}
