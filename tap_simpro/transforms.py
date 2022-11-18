import re


def transform_catalogs(row):
    regex = r"^(.+?)(?: non? catalog item)? - Invoice (.+?)$"
    matches = re.findall(regex, row["Name"])

    if matches:
        [(supplier, invoice_number)] = matches
        row["Supplier"] = supplier
        row["InvoiceNumber"] = invoice_number


transforms = {
    "catalogs": transform_catalogs,
}
