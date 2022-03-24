streams = {
    "contractors": ["contractor_timesheets"],
    "customers": ["customer_sites"],
    "employees": ["employee_timesheets"],
    "invoices": ["invoice_jobs"],
    "jobs": [
        "job_sections",
        "job_cost_centers",
        "job_cost_center_catalog_item",
        "job_cost_center_labor_item",
        "job_cost_center_one_off_item",
        "job_cost_center_prebuild_item",
        "job_cost_center_service_fee",
    ],
    # disabled for now as the pagination is buggy
    # "payable_invoices": ["payable_invoices_cost_centers"],
    "schedules": ["schedules_blocks"],
    "quotes": ["quote_sections", "quote_cost_centers"],
}

has_details = {"payable_invoices": False}

json_encoded_columns = {
    "jobs": ["RequestNo", "Name", "Description", "Notes"],
    "quotes": ["RequestNo", "Name", "Description", "Notes"],
}

resource_details_url_fns = {
    "jobs": lambda row: f'jobs/{row["ID"]}?display=all',
    "quotes": lambda row: f'quotes/{row["ID"]}?display=all',
}
