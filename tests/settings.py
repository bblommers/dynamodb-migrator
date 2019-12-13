import os

CONNECT_TO_AWS = os.environ.get("CONNECT_TO_AWS", "false").lower() == "true"
