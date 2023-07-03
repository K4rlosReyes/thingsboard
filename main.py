from Telemetry.TelemetryGetter import TelemetryGetter
from Telemetry.TelemetrySaver import TelemetrySaver

# ThingsBoard REST API URL
URL = "https://thingsboard.cloud"

# Default Tenant Administrator credentials
USERNAME = "pfernandez@mat.upv.es"
PASSWORD = "123456"
CUSTOMER_ID = "89ffe890-0e38-11ec-a4b0-6fb4a09a8a57"
TIMESERIES = 'CO2'
DEVICE_LABELS = ["CUD-01", "CUD-02"]


# configure the module
telgetter = TelemetryGetter()
telgetter.set_url(url=URL)
telgetter.set_credentials(username=USERNAME, password=PASSWORD)
telgetter.set_customer_id( customer_id=CUSTOMER_ID )

# Fetch telemetry data
ldev = telgetter.fetch_telemetry( devices_label=DEVICE_LABELS, timeseries_key=TIMESERIES )

# Save Telemetry data
saver = TelemetrySaver( database_address='database_test.db', table_name='telemetry' )
saver.save_telemetry( ldev )
saver.close()