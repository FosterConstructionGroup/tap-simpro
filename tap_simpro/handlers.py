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


async def handle_contractor_timesheets(session, contractor, schemas, state, mdata):
    resource = "contractor_timesheets"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    id = contractor["ID"]
    url = f"contractors/{id}/timesheets/?Includes=Job,Activity"

    await handle_timesheets(
        session, resource, id, url, schema, state, mdata, extraction_time
    )


async def handle_credit_note_jobs(session, credit_note, schemas, state, mdata):
    resource = "credit_note_jobs"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    for j in credit_note["Jobs"]:
        j["CreditNoteID"] = credit_note["ID"]
        # rename row ID to JobID so it's clearer
        j["JobID"] = j["ID"]
        j["ID"] = str(j["CreditNoteID"]) + "_" + str(j["JobID"])

        write_record(j, resource, schema, mdata, extraction_time)


async def handle_credit_note_cost_centers(session, credit_note, schemas, state, mdata):
    resource = "credit_note_cost_centers"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    for cc in credit_note["CostCenters"]:
        cc["CreditNoteID"] = credit_note["ID"]
        # rename row ID so it's clearer
        cc["JobCostCenterID"] = cc["ID"]
        cc["ID"] = str(cc["CreditNoteID"]) + "_" + str(cc["JobCostCenterID"])

        write_record(cc, resource, schema, mdata, extraction_time)


async def handle_customer_sites(session, row, schemas, state, mdata):
    resource = "customer_sites"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    for site in row["Sites"]:
        record = {
            "ID": row["ID"] + site["ID"],
            "CustomerID": row["ID"],
            "SiteID": site["ID"],
        }
        write_record(record, resource, schema, mdata, extraction_time)


async def handle_employee_timesheets(session, employee, schemas, state, mdata):
    resource = "employee_timesheets"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    id = employee["ID"]
    url = f"employees/{id}/timesheets/?Includes=Job,Activity"

    await handle_timesheets(
        session, resource, id, url, schema, state, mdata, extraction_time
    )


async def handle_invoice_jobs(session, invoice, schemas, state, mdata):
    resource = "invoice_jobs"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    for j in invoice["Jobs"]:
        j["InvoiceID"] = invoice["ID"]
        # rename row ID to JobID so it's clearer
        j["JobID"] = j["ID"]
        j["ID"] = str(j["InvoiceID"]) + "_" + str(j["JobID"])

        write_record(j, resource, schema, mdata, extraction_time)


async def handle_invoice_cost_centers(session, invoice, schemas, state, mdata):
    resource = "invoice_cost_centers"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    for cc in invoice["CostCenters"]:
        cc["InvoiceID"] = invoice["ID"]
        # rename row ID so it's clearer
        cc["JobCostCenterID"] = cc["ID"]
        cc["ID"] = str(cc["InvoiceID"]) + "_" + str(cc["JobCostCenterID"])

        write_record(cc, resource, schema, mdata, extraction_time)


async def handle_job_tags(session, job, schemas, state, mdata):
    resource = "job_tags"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    for tag in job.get("Tags", []):
        tag["JobID"] = job["ID"]
        tag["TagID"] = tag["ID"]
        tag["ID"] = str(tag["JobID"]) + "_" + str(tag["TagID"])

        write_record(tag, resource, schema, mdata, extraction_time)


async def handle_job_sections_cost_centers(session, job, schemas, state, mdata):
    s_resource = "job_sections"
    s_schema = schemas[s_resource]
    c_resource = "job_cost_centers"
    c_schema = schemas.get(c_resource)
    extraction_time = datetime.now(timezone.utc).astimezone()

    items_handlers = {
        "job_cost_center_catalog_item": "catalogs",
        "job_cost_center_labor_item": "labor",
        "job_cost_center_one_off_item": "oneOffs",
        "job_cost_center_prebuild_item": "prebuilds",
        "job_cost_center_service_fee": "serviceFees",
    }

    # far better parallelism getting everything at once than using `await` at the cost center granularity
    items_futures = []

    for s in job["Sections"]:
        s["JobID"] = job["ID"]
        write_record(s, s_resource, s_schema, mdata, extraction_time)

        if c_resource in schemas:
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

    await await_futures(items_futures)


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
        async for row in get_resource(
            session, resource, bookmark, schema, endpoint_override=endpoint
        ):
            write_record({**row, **path_vars}, resource, schema, mdata, extraction_time)
    # service fees can throw a 404 instead of just returning [], so handle that case
    except ClientResponseError as e:
        if e.status == 404:
            pass
        else:
            raise e


async def handle_payable_invoices_cost_centers(session, invoice, schemas, state, mdata):
    resource = "payable_invoices_cost_centers"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

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


async def handle_quote_sections_cost_centers(session, quote, schemas, state, mdata):
    s_resource = "quote_sections"
    s_schema = schemas[s_resource]
    c_resource = "quote_cost_centers"
    c_schema = schemas.get(c_resource)

    extraction_time = datetime.now(timezone.utc).astimezone()

    new_bookmarks = {s_resource: extraction_time}

    for s in quote["Sections"]:
        s["QuoteID"] = quote["ID"]
        write_record(s, s_resource, s_schema, mdata, extraction_time)

        if c_resource in schemas:
            new_bookmarks[c_resource] = extraction_time
            for c in s["CostCenters"]:
                c["QuoteID"] = quote["ID"]
                c["SectionID"] = s["ID"]
                write_record(c, c_resource, c_schema, mdata, extraction_time)


async def handle_schedules_blocks(session, row, schemas, state, mdata):
    resource = "schedules_blocks"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    i = 0
    for block in row["Blocks"]:
        i += 1
        id = row["ID"]
        block["ID"] = f"{id}_{i}"
        block["ScheduleID"] = id
        write_record(block, resource, schema, mdata, extraction_time)


async def handle_task_assignees(session, row, schemas, state, mdata):
    resource = "task_assignees"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    for a in row.get("Assignees", []):
        a["AssigneeID"] = a["ID"]
        a["TaskID"] = row["ID"]
        a["ID"] = f'{a["TaskID"]}_{a["AssigneeID"]}'
        write_record(a, resource, schema, mdata, extraction_time)


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
    session, vendor_order, schemas, state, mdata
):
    parent_resource = "vendor_orders"
    parent_bookmark = get_bookmark(state, parent_resource, "since")

    # vendor_orders stream includes all POs (whether they've changed or not) as PO receipts and credits date modified aren't linked to the PO so would be easy to not sync these and have subtly wrong data
    # PO items don't have their own date modified so assuming it's the same as the parent, so can return early to make less API calls
    if parent_bookmark and vendor_order["DateModified"] <= parent_bookmark:
        return

    resource = "vendor_order_item_allocations"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    async def handler(v):
        endpoint = f'vendorOrders/{v["ID"]}/catalogs'

        async for r in get_resource(
            session,
            resource,
            None,
            schema,
            endpoint_override=endpoint,
            get_details_url=lambda row: f"{endpoint}/{row['Catalog']['ID']}",
        ):
            for a in r["Allocations"]:
                a["VendorOrderID"] = v["ID"]
                a["CostCenterID"] = v.get("AssignedTo", {}).get("ID")
                a["Catalog"] = r["Catalog"]
                a["Price"] = r["Price"]
                a["ID"] = f'{v["ID"]}_{r["Catalog"]["ID"]}'

            yield r

    flattened = [a async for row in handler(vendor_order) for a in row["Allocations"]]
    write_many(flattened, resource, schema, mdata, extraction_time)


async def handle_vendor_order_receipts(session, vendor_order, schemas, state, mdata):
    r_resource = "vendor_order_receipts"
    r_schema = schemas[r_resource]
    i_resource = "vendor_order_receipt_items"
    i_schema = schemas.get(i_resource)

    bookmark = get_bookmark(state, r_resource, "since")
    extraction_time = datetime.now(timezone.utc).astimezone()

    async for r in get_resource(
        session,
        r_resource,
        bookmark,
        r_schema,
        endpoint_override=f'vendorOrders/{vendor_order["ID"]}/receipts',
    ):
        # helpful for credits
        r["VendorOrderID"] = r["VendorOrderNo"]
        write_record(r, r_resource, r_schema, mdata, extraction_time)

        if i_resource in schemas:
            for c in r["Catalogs"]:
                # reset index with each new catalog item; only want to increment through the array
                i = 0
                for item in c["Allocations"]:
                    i += 1
                    item["VendorOrderReceiptID"] = r["ID"]
                    item["VendorOrderID"] = r["VendorOrderNo"]
                    item["CatalogID"] = c["Catalog"]["ID"]
                    item["ID"] = f'{r["ID"]}_{c["Catalog"]["ID"]}_{i}'
                    write_record(item, i_resource, i_schema, mdata, extraction_time)

        if "vendor_order_credits" in schemas:
            await handle_vendor_order_credits(session, r, schemas, state, mdata)


async def handle_vendor_order_credits(
    session, vendor_order_receipt, schemas, state, mdata
):
    c_resource = "vendor_order_credits"
    c_schema = schemas[c_resource]
    i_resource = "vendor_order_credit_items"
    i_schema = schemas.get(i_resource)

    bookmark = get_bookmark(state, c_resource, "since")
    extraction_time = datetime.now(timezone.utc).astimezone()

    # separate function is the cleanest way to add the two parent reference fields
    async def get_credits(r):
        async for c in get_resource(
            session,
            c_resource,
            bookmark,
            c_schema,
            endpoint_override=f'vendorOrders/{r["VendorOrderID"]}/receipts/{r["ID"]}/credits',
        ):
            c["VendorOrderID"] = r["VendorOrderID"]
            c["VendorOrderReceiptID"] = r["ID"]
            yield c

    async def get_items(c):
        endpoint = f'vendorOrders/{c["VendorOrderID"]}/receipts/{c["VendorOrderReceiptID"]}/credits/{c["ID"]}/catalogs'
        async for i in get_resource(
            session,
            i_resource,
            None,
            i_schema,
            endpoint_override=endpoint,
        ):
            i["VendorOrderID"] = c["VendorOrderID"]
            i["VendorOrderReceiptID"] = c["VendorOrderReceiptID"]
            i["VendorOrderCreditID"] = c["ID"]
            i["ID"] = f'{i["VendorOrderCreditID"]}_{i["Catalog"]["ID"]}'
            yield i

    async for credit in get_credits(vendor_order_receipt):
        write_record(credit, c_resource, c_schema, mdata, extraction_time)

        if i_resource in schemas:
            async for item in get_items(credit):
                write_record(item, i_resource, i_schema, mdata, extraction_time)


async def handle_job_work_order_blocks(session, row, schemas, state, mdata):
    resource = "job_work_order_blocks"
    schema = schemas[resource]
    extraction_time = datetime.now(timezone.utc).astimezone()

    i = 0
    for block in row["Blocks"]:
        i += 1
        id = row["ID"]
        block["ID"] = f"{id}_{i}"
        block["JobWorkOrderID"] = id
        write_record(block, resource, schema, mdata, extraction_time)


handlers = {
    "contractor_timesheets": handle_contractor_timesheets,
    "credit_note_jobs": handle_credit_note_jobs,
    "credit_note_cost_centers": handle_credit_note_cost_centers,
    "customer_sites": handle_customer_sites,
    "employee_timesheets": handle_employee_timesheets,
    "invoice_jobs": handle_invoice_jobs,
    "invoice_cost_centers": handle_invoice_cost_centers,
    "job_tags": handle_job_tags,
    "job_sections": handle_job_sections_cost_centers,
    "job_work_order_blocks": handle_job_work_order_blocks,
    # job_cost_centers and children are sub-streams to job_sections so can't be called directly
    "payable_invoices_cost_centers": handle_payable_invoices_cost_centers,
    "schedules_blocks": handle_schedules_blocks,
    "quote_sections": handle_quote_sections_cost_centers,
    # quote_cost_centers is a sub-stream to quote_sections so can't be called directly
    "task_assignees": handle_task_assignees,
    "vendor_order_item_allocations": handle_vendor_order_item_allocations,
    "vendor_order_receipts": handle_vendor_order_receipts,
    # vendor_order_receipt_items, vendor_order_credits, vendor_order_credit_items are sub-streams to vendor_order_receipts so can't be called directly
}
