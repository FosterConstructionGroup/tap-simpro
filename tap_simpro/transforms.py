import re

from .handlers import recurring_invoice_ids_synced, recurring_invoice_ids_seen


def transform_catalogs(rows):
    regex = r"^(.+?)(?: non? catalog item)? - Invoice (.+?)$"

    for row in rows:
        matches = re.findall(regex, row["Name"])

        if matches:
            [(supplier, invoice_number)] = matches
            row["Supplier"] = supplier
            row["InvoiceNumber"] = invoice_number


def transform_invoices(rows):
    for r in rows:
        recurring_invoice = r.get("RecurringInvoice")
        if recurring_invoice:
            recurring_invoice_ids_seen.add(recurring_invoice["ID"])


def transform_recurring_invoices(rows):
    for r in rows:
        recurring_invoice_ids_synced.add(r["ID"])


transforms = {
    "catalogs": transform_catalogs,
    "invoices": transform_invoices,
    "recurring_invoices": transform_recurring_invoices,
}
