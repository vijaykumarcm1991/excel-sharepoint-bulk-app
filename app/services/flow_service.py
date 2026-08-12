import logging
import requests
from app.config import FLOW_URL

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 30  # seconds


def send_to_flow(payload):
    if not FLOW_URL:
        error_msg = "FLOW_URL is not configured"
        logger.error(error_msg)
        return 500, {"error": error_msg}

    try:
        response = requests.post(FLOW_URL, json=payload, timeout=REQUEST_TIMEOUT)
    except requests.exceptions.Timeout:
        error_msg = f"Flow request timed out after {REQUEST_TIMEOUT}s"
        logger.error(error_msg)
        return 504, {"error": error_msg}
    except requests.exceptions.ConnectionError as e:
        error_msg = f"Flow connection error: {e}"
        logger.error(error_msg)
        return 503, {"error": error_msg}
    except requests.exceptions.RequestException as e:
        error_msg = f"Flow request error: {e}"
        logger.error(error_msg)
        return 500, {"error": error_msg}

    try:
        return response.status_code, response.json()
    except ValueError:
        return response.status_code, {"error": f"Invalid JSON response from Flow (status {response.status_code})"}
