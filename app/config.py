import os
from dotenv import load_dotenv

load_dotenv()

FLOW_URL = os.getenv("FLOW_URL")

if not FLOW_URL:
    raise RuntimeError(
        "FLOW_URL environment variable is not set. "
        "Create a .env file with FLOW_URL=<your_power_automate_flow_url>."
    )
