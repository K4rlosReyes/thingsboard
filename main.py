from Telemetry.TelemetryGetter import TelemetryGetter
from Telemetry.TelemetrySaver import TelemetrySaver
from dotenv import load_dotenv
import os

load_dotenv()

# ThingsBoard REST API URL
URL = os.getenv("URL")

# Default Tenant Administrator credentials
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
CUSTOMER_ID = os.getenv("CUSTOMER_ID")
TIMESERIES = os.getenv("TIMESERIES")
DEVICE_LABELS = os.getenv("DEVICE_LABELS")


# configure the module
telgetter = TelemetryGetter()
telgetter.set_url(url=URL)
telgetter.set_credentials(username=USERNAME, password=PASSWORD)
telgetter.set_customer_id(customer_id=CUSTOMER_ID)

# Fetch telemetry data
ldev = telgetter.fetch_telemetry(devices_label=DEVICE_LABELS, timeseries_key=TIMESERIES)

# Save Telemetry data
saver = TelemetrySaver(database_address="database_test.db", table_name="telemetry")
saver.save_telemetry(ldev)
saver.close()
