streams = {
    "contractors": ["contractor_timesheets"],
    "customers": ["customer_sites"],
    "employees": ["employee_timesheets"],
    "invoices": ["invoice_jobs"],
    "jobs": [
        "job_tags",
        "job_sections",
        "job_cost_centers",
        "job_cost_center_catalog_item",
        "job_cost_center_labor_item",
        "job_cost_center_one_off_item",
        "job_cost_center_prebuild_item",
        "job_cost_center_service_fee",
    ],
    "job_work_orders": ["job_work_order_blocks"],
    # disabled for now as the pagination is buggy
    # "payable_invoices": ["payable_invoices_cost_centers"],
    "schedules": ["schedules_blocks"],
    "quotes": ["quote_sections", "quote_cost_centers"],
    "vendor_orders": [
        "vendor_order_item_allocations",
        "vendor_order_receipts",
        "vendor_order_receipt_items",
        "vendor_order_credits",
        "vendor_order_credit_items",
    ],
}

streams_with_details = {"payable_invoices": False}

# handler doesn't fetch details for these either
# note that this doesn't work well with array sub-streams as they aren't in the parent schema so don't get returned in the response. Don't want to add them to the parent schema either
streams_specify_columns = set(
    [
        "accounts",
        "activities",
        "catalogs",
        "contacts",
        "employees",
        "invoices",
        "schedules",
        "sites",
        "vendor_order_credit_items",
        "vendor_order_credits",
        "vendor_order_item_allocations",
        "vendor_order_receipts",
        "vendor_orders",
        "vendors",
    ]
)
streams_add_specified_columns = {
    "invoices": ",Jobs",
    "schedules": ",Blocks",
    "vendor_order_item_allocations": ",Allocations",
    "vendor_order_receipts": ",Catalogs",
}
# Typically part of the URL but not present in the response
streams_exclude_specified_columns = {
    "vendor_order_credits": set(["VendorOrderID", "VendorOrderReceiptID"]),
    "vendor_order_credit_items": set(
        ["ID", "VendorOrderID", "VendorOrderReceiptID", "VendorOrderCreditID"]
    ),
}

json_encoded_columns = {
    "jobs": ["RequestNo", "Name", "Description", "Notes"],
    "quotes": ["RequestNo", "Name", "Description", "Notes"],
}

resource_details_url_fns = {
    "jobs": lambda row: f'jobs/{row["ID"]}?display=all',
    "quotes": lambda row: f'quotes/{row["ID"]}?display=all',
}
