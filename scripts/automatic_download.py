import logging
import sqlite3
from datetime import datetime

# Importing models and REST client class from Professional Edition version
from tb_rest_client.rest_client_pe import *
from tb_rest_client.rest import ApiException

import time

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ThingsBoard REST API URL
url = "https://thingsboard.cloud"

# Default Tenant Administrator credentials
username = "pfernandez@mat.upv.es"
password = "123456"
customer_id = "89ffe890-0e38-11ec-a4b0-6fb4a09a8a57"

device_labels = ["CUD-01", "CUD-02"]

# Crear una conexión a la base de datos
conn = sqlite3.connect("telemetry.db")
cursor = conn.cursor()


def get_filtered_devices(rest_client, devices, labels):
    filtered_devices = []
    for device in devices.data:
        device_details = rest_client.get_device_by_id(device.id.id)
        device_label = device_details.label
        if device_label in labels:
            filtered_devices.append(device)
    return filtered_devices


def save_telemetry_data(telemetry_data):
    for device_name, telemetry in telemetry_data.items():
        for ts_key, ts_values in telemetry.items():
            table_name = f"telemetry_{device_name}_{ts_key}"
            timestamp = datetime.fromtimestamp(int(ts_key) // 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            for key_value in ts_values:
                key = key_value["key"]
                value = key_value["value"]
                cursor.execute(
                    f"INSERT INTO {table_name} (timestamp, value) VALUES (?, ?)",
                    (timestamp, value),
                )
                cursor.execute(
                    "INSERT INTO telemetry_data (device_name, timestamp, key, value) VALUES (?, ?, ?, ?)",
                    (device_name, timestamp, key, value),
                )
    conn.commit()


# Creating the REST client object with context manager to get auto token refresh
with RestClientPE(base_url=url) as rest_client:
    try:
        # Auth with credentials
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
                device_keys = ["Temperatura", "CO2", "Humedad", "Dispositivos"]

                logging.info("Getting telemetry")
                telemetry = rest_client.telemetry_controller.get_timeseries_using_get(
                    "DEVICE",
                    device_id,
                    keys=device_keys,
                    start_ts=int(time.time() * 1000) - 24 * 60 * 60 * 1000,
                    end_ts=int(time.time() * 1000),
                )
                telemetry_data[device.label] = telemetry

            save_telemetry_data(telemetry_data)
            logging.info("Telemetry Saved")

            time.sleep(
                24 * 60 * 60
            )  # Esperar 24 horas antes de la siguiente actualización

    except ApiException as e:
        logging.exception(e)

# Cerrar la conexión a la base de datos
conn.close()
