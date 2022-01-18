streams = {
    "contractors": ["contractor_timesheets"],
    "customers": ["customer_sites"],
    "employees": ["employee_timesheets"],
    "invoices": ["invoice_jobs"],
    "schedules": ["schedules_blocks"],
    # disabled for now as the pagination is buggy
    # "payable_invoices": ["payable_invoices_cost_centers"],
    "jobs": ["job_sections", "job_cost_centers"],
}

has_details = {"payable_invoices": False}
