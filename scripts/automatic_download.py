import logging
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
from tb_rest_client.rest_client_pe import RestClientPE
from tb_rest_client.rest import ApiException
import os

import time

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ThingsBoard REST API URL
url = "https://thingsboard.cloud"

# Default Tenant Administrator credentials
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")
customer_id = "89ffe890-0e38-11ec-a4b0-6fb4a09a8a57"

# Agregar todos los dispositivos que se deseen descargar sus datos
device_labels = [
    "CUD-01",
    "CUD-02",
]


def create_tables(conn, device_labels):
    cursor = conn.cursor()
    with RestClientPE(base_url=url) as rest_client:
        try:
            # Authenticate with credentials
            rest_client.login(username=username, password=password)

            devices = rest_client.get_customer_devices(
                customer_id=customer_id, page_size=10, page=0
            )

            for device in devices.data:
                if device.label in device_labels:
                    device_keys = ["Temperatura", "CO2", "Humedad", "Dispositivos"]

                    for key in device_keys:
                        table_name = f"telemetry_{key}"
                        cursor.execute(
                            f"""
                            CREATE TABLE IF NOT EXISTS {table_name} (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                device_name TEXT,
                                timestamp TIMESTAMP,
                                value FLOAT
                            )
                        """
                        )

            conn.commit()

        except ApiException as e:
            logging.exception(e)


def get_filtered_devices(rest_client, devices, labels):
    filtered_devices = []
    for device in devices.data:
        device_details = rest_client.get_device_by_id(device.id.id)
        device_label = device_details.label
        if device_label in labels:
            filtered_devices.append(device)
    return filtered_devices


def save_telemetry_data(conn, telemetry_data):
    cursor = conn.cursor()
    for device_name, telemetry in telemetry_data.items():
        for ts_key, ts_values in telemetry.items():
            table_name = f"telemetry_{ts_key}"

            for key_value in ts_values:
                value = key_value["value"]
                timestamp = key_value["ts"]
                cursor.execute(
                    f"INSERT INTO {table_name} (device_name, timestamp, value) VALUES (?, ?, ?)",
                    (device_name, timestamp, value),
                )
    conn.commit()


# Create a database connection
conn = sqlite3.connect("telemetry.db")

# Create tables for each parameter of each device
create_tables(conn, device_labels)

# Creating the REST client object with context manager to get auto token refresh
with RestClientPE(base_url=url) as rest_client:
    try:
        # Authenticate with credentials
        rest_client.login(username=username, password=password)

        devices = rest_client.get_customer_devices(
            customer_id=customer_id, page_size=10, page=0
        )
        filtered_devices = get_filtered_devices(rest_client, devices, device_labels)

        logging.info("Devices: \n%r", len(filtered_devices))

        while True:
            telemetry_data = {}

            for device in filtered_devices:
                device_id = device.id.id
                device_details = rest_client.get_device_by_id(device_id)
                # device_keys = rest_client.get_timeseries_keys(device_id)
                device_keys = "Temperatura,CO2,Humedad,Dispositivos"

                logging.info("Getting telemetry")
                telemetry = rest_client.telemetry_controller.get_timeseries_using_get(
                    "DEVICE",
                    device_id,
                    keys=device_keys,
                    start_ts=int(time.time() * 1000) - 24 * 60 * 60 * 1000,
                    end_ts=int(time.time() * 1000),
                )
                telemetry_data[device.label] = telemetry

            save_telemetry_data(conn, telemetry_data)
            logging.info("Telemetry Saved")

            time.sleep(24 * 60 * 60)  # Wait 24 hours until the next update

    except ApiException as e:
        logging.exception(e)

# Close the database connection
conn.close()
