import logging
from datetime import datetime

from tb_rest_client.rest_client_pe import RestClientPE
from tb_rest_client.rest import ApiException

import time

logging.basicConfig(
    filename="logs/server_logs.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class TelemetryGetter(object):
    def __init__(self):
        """
        Initialize all class attributes
        """

        self.url = ""
        self.username = ""
        self.password = ""
        self.customer_id = ""

    def set_url(self, url: str) -> None:
        """Set "asd" Thingsboard url"""
        self.url = url

    def set_credentials(self, username: str, password: str) -> None:
        """Set credentials"""
        self.username = username
        self.password = password

    def set_customer(self, customer: str):
        """Set customer by label or name"""
        # TODO: Pending
        pass

    def set_customer_id(self, customer_id: str):
        """Set customer id"""
        self.customer_id = customer_id

    def __filter_devices(self, all_devices: list, devices_labels: list) -> list:
        filtered_devices = []
        for device in all_devices:
            # device_details = self.rest_client.get_device_by_id(device.id.id)
            try:
                device_label = self.rest_client.get_device_by_id(device.id.id).label
                if device_label in devices_labels:
                    filtered_devices.append(device)
            except ApiException as e:
                logging.exception(e)

        return filtered_devices

    def fetch_telemetry(self, devices_label: list, timeseries_key: str, timestamp: int):
        """
        Fetch telemetry data from Thingsboard
        
        Return (None, None) in case of error
        """
        self.rest_client = RestClientPE(base_url=self.url)
        try:
            # Authenticate with credentials
            self.rest_client.login(username=self.username, password=self.password)
        except ApiException as e:
            logging.exception(e)
            print("Login error")
            return (None,None)


		# Get customer devices
        page = 0
        all_devices = list()
        try:
            while True:
                devices = self.rest_client.get_customer_devices(
                    customer_id=self.customer_id, page_size=10, page=page
                )

                for device in devices.data:
                    all_devices.append(device)

                if devices.has_next == False:
                    break
                page += 1
        except ApiException as e:
            logging.exception(e)
            print("error getting customer devices")
            return (None,None)
            
            # filter device by label
        filtered_devices = self.__filter_devices(all_devices, devices_label)
        telemetry_data = {}

        # specify interval time
        t = int(time.time()) * 1000
        st = timestamp
        # st = int(time.time()) * 1000 - 24 * 60 * 60 * 1000

        logging.info(f"st: {st}, {type(st)}")
        logging.info(f"en: {t}, {type(t)}")

        logging.info(f"Getting telemetry from {len(filtered_devices)} devices")
        for device in filtered_devices:
            device_id = device.id.id
            device_details = self.rest_client.get_device_by_id(device_id)
            device_keys = timeseries_key
            device_keys = device_keys.split(",")            
            logging.info(f"Getting telemetry from device {device.label}")
            try:
                telemetry = (self.rest_client.telemetry_controller.get_timeseries_using_get(
                        "DEVICE",
                        device_id,
                        keys=timeseries_key,
                        start_ts=st,
                        end_ts=t,
                    ))
                
                for key in device_keys:
                    try:
                        l = len(telemetry.get(key))
                    except:
                        l = 0
                    logging.info(f"Received {l} samples of {key}")
                        
                telemetry_data[device.label] = telemetry
            except ApiException as e:
                print("get_timeseries_using_get error")
                logging.exception( e )

        return (telemetry_data, t)
