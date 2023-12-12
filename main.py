import Telemetry.TelemetryGetter
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
DEVICE_LABELS = DEVICE_LABELS.split(",")
DATABASE_ADDRESS = os.getenv("DATABASE_ADDRESS")

# configure the module
telgetter = TelemetryGetter()
telgetter.set_url(url=URL)
telgetter.set_credentials(username=USERNAME, password=PASSWORD)
telgetter.set_customer_id(customer_id=CUSTOMER_ID)

# Initializing saver object
saver = TelemetrySaver(database_address=DATABASE_ADDRESS, table_name="telemetry")

# Get last timestamp recorded
timestamp = saver.get_last_timestamp()
print( f"saver timestamp: {timestamp}" )
# Fetch telemetry data
(ldev, ts) = telgetter.fetch_telemetry(
    devices_label=DEVICE_LABELS, timeseries_key=TIMESERIES, timestamp=timestamp
)
if ldev != None and ts != None:
	# Save Telemetry data
	saver.save_telemetry(ldev, ts)
	# Closing db connection
	saver.close()
	print("Data saved!!!")
else:
	print("No data acquired!!!")