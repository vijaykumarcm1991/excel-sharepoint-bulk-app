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
                results.append({
                    "row": index + 2,
                    "status": "Failed",
                    "reason": "Invalid Product Name"
                })
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

            if status_code == 200:
                results.append({
                    "row": index + 2,
                    "IncidentID": payload["IncidentID"],
                    "status": flow_result.get("status", "Created")
                })
            else:
                results.append({
                    "row": index + 2,
                    "IncidentID": payload["IncidentID"],
                    "status": "Failed",
                    "reason": flow_result.get("error", "Unknown error")
                })

        except Exception as e:
            results.append({
                "row": index + 2,
                "status": "Error",
                "reason": str(e)
            })

    # Remove old failure file if exists
    failure_file_path = "/tmp/failures.xlsx"
    if os.path.exists(failure_file_path):
        os.remove(failure_file_path)

    # --- Generate Failure Report ---
    result_df = pd.DataFrame(results)

    failure_df = result_df[result_df["status"] == "Failed"]

    if not failure_df.empty:
        failure_df.to_excel("/tmp/failures.xlsx", index=False)

    return {"summary": results}