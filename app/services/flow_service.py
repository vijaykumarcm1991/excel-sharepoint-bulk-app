import requests
from app.config import FLOW_URL

def send_to_flow(payload):
    response = requests.post(FLOW_URL, json=payload)
    try:
        return response.status_code, response.json()
    except:
        return response.status_code, {}
