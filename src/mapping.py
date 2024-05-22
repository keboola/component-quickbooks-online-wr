import logging

from client import QuickbooksClientException

expected_columns = {
    "journalentry": {
        "create": ["Id", "Type", "TxnDate", "PrivateNote", "AccountRefName", "AccountRefValue", "Amount",
                   "Description", "ClassRefName", "DepartmentRefName", "ClassRefValue", "DepartmentRefValue",
                   "EntityName", "DocNumber"]
    }
}


def create_entries(endpoint: str, action, data: list) -> dict:
    entries = {}

    if endpoint == "journalentry" and action == "create":
        entries["TxnDate"] = data[0]["TxnDate"]
        entries["DocNumber"] = data[0]["DocNumber"]
        entries["PrivateNote"] = data[0]["PrivateNote"]

        lines = []

        for row in data:

            line_detail = {
                "PostingType": row["Type"],
                "AccountRef": {
                    "name": row.get("AccountRefName"),
                    "value": row["AccountRefValue"]
                }
            }

            if row.get("ClassRefValue"):
                line_detail["ClassRef"] = {
                    "name": row.get("ClassRefName"),
                    "value": row.get("ClassRefValue")
                }

            if row.get("DepartmentRefValue"):
                line_detail["DepartmentRef"] = {
                    "name": row.get("DepartmentRefName"),
                    "value": row.get("DepartmentRefValue")
                }

            additional_line_detail = {
                "JournalEntryLineDetail": line_detail,
                "DetailType": "JournalEntryLineDetail",
                "Amount": float(row["Amount"]),
                "Description": row["Description"]
            }

            lines.append(additional_line_detail)

        entries["Line"] = lines
        logging.debug(f"Entries: {entries}")
        return entries

    raise QuickbooksClientException(f"Unsupported endpoint and action: {endpoint}, {action}")
