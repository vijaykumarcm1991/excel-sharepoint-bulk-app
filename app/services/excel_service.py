import pandas as pd
from io import BytesIO
from app.utils.product_mapper import PRODUCT_MAP
from app.services.flow_service import send_to_flow
import os

REQUIRED_COLUMNS = [
    "ListName",
    "IncidentID",
    "ProductName",
    "AuditPeriod",
    "AssigneeName",
    "AuditedByName"
]

def safe_value(value):
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.strftime("%b - %Y")   # Jan - 2026 format

    return str(value).strip()

def process_excel(file_bytes):
    df = pd.read_excel(BytesIO(file_bytes), engine="openpyxl")

    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        return {"error": f"Missing columns: {missing_cols}"}

    results = []

    for index, row in df.iterrows():
        try:
            product_name = str(row["ProductName"]).strip().upper()

            if product_name not in PRODUCT_MAP:
                row_result = row.to_dict()
                row_result["Status"] = "Failed"
                row_result["Reason"] = "Invalid Product Name"
                results.append(row_result)
                continue

            payload = {
                "ListName": safe_value(row["ListName"]),
                "IncidentID": safe_value(row["IncidentID"]),
                "ProductId": PRODUCT_MAP[product_name],
                "AuditPeriod": safe_value(row["AuditPeriod"]),
                "AssigneeName": safe_value(row["AssigneeName"]),
                "AuditedByName": safe_value(row["AuditedByName"])
            }

            status_code, flow_result = send_to_flow(payload)

            row_result = {col: safe_value(row[col]) for col in row.index}  # get full Excel row

            if status_code == 200:
                row_result["Status"] = flow_result.get("status", "Created")
                row_result["Reason"] = ""
            else:
                row_result["Status"] = "Failed"
                row_result["Reason"] = flow_result.get("error", "Unknown error")

            results.append(row_result)

        except Exception as e:
            row_result = row.to_dict()
            row_result["Status"] = "Error"
            row_result["Reason"] = str(e)
            results.append(row_result)

    # Remove old failure file if exists
    failure_file_path = "/tmp/failures.xlsx"
    if os.path.exists(failure_file_path):
        os.remove(failure_file_path)

    # --- Generate Failure Report ---
    result_df = pd.DataFrame(results)

    failure_df = result_df[result_df["Status"].isin(["Failed", "Error"])]

    if not failure_df.empty:
        failure_df.to_excel(failure_file_path, index=False)

    return {"summary": results}