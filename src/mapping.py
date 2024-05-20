expected_columns = {
    "journalentry":
        ["GroupId", "Type", "TxnDate", "PrivateNote", "AcctNum", "FullyQualifiedName", "Amount", "Description", "Id",
         "ClassRefName", "DepartmentRefName", "ClassRefValue", "DepartmentRefValue", "EntityName", "DocNumber"]
}


def create_entries(endpoint: str, data: list) -> dict:
    entries = {}
    if endpoint == "journalentry":
        entries["TxnDate"] = data[0]["TxnDate"]
        entries["DocNumber"] = data[0]["DocNumber"]
        entries["PrivateNote"] = data[0]["PrivateNote"]

        lines = []

        for row in data:

            line_detail = {
                "PostingType": row["Type"],
                "AccountRef": {
                    "name": row.get("FullyQualifiedName"),
                    "value": row["AcctNum"]
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

            lines.append({
                "JournalEntryLineDetail": line_detail,
                "DetailType": "JournalEntryLineDetail",
                "Amount": float(row["Amount"]),
                "Description": row["Description"]
            })

        entries["Line"] = lines
        return entries

    raise Exception(f"Unsupported endpoint: {endpoint}")
