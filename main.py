from Telemetry.TelemetryGetter import TelemetryGetter
from Telemetry.TelemetrySaver import TelemetrySaver
from dotenv import load_dotenv
import os

load_dotenv()

# ThingsBoard REST API URL
URL = os.getenv("URL")

# TODO: Comment every variable properly
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

# Initializing saver object
saver = TelemetrySaver(database_address="database_test.db", table_name="telemetry")

# Get last timestamp recorded
timestamp = saver.get_timestamp()

# Fetch telemetry data
(ldev, ts) = telgetter.fetch_telemetry(
    devices_label=DEVICE_LABELS, timeseries_key=TIMESERIES, timestamp=timestamp
)

# Save Telemetry data
saver.save_telemetry(ldev, ts)

# Closing db connection
saver.close()
