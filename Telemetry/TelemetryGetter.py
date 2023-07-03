import logging
from datetime import datetime

from tb_rest_client.rest_client_pe import RestClientPE
from tb_rest_client.rest import ApiException

import time

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class TelemetryGetter(object):
    
    def __init__(self):
        '''
        Initialize all class attributes
        '''

        self.url = ''
        self.username = ''
        self.password = ''
        self.customer_id = ''


    def set_url(self, url:str ) -> None:
        '''Set Thingsboard url'''
        self.url = url
    
    def set_credentials( self, username: str, password: str ) ->None:
        '''Set credentials'''
        self.username = username
        self.password = password
    
    def set_customer( self, customer: str ):
        '''Set customer by label or name'''
        # TODO: Pending
        pass

    def set_customer_id( self, customer_id: str ):
        '''Set customer id'''
        self.customer_id = customer_id

    def __filter_devices( self, all_devices:list, devices_labels: list ) -> list:
        filtered_devices = []
        for device in all_devices:
            #device_details = self.rest_client.get_device_by_id(device.id.id)
            device_label = self.rest_client.get_device_by_id(device.id.id).label
            if device_label in devices_labels:
                filtered_devices.append(device)
        return filtered_devices

    def fetch_telemetry(self, devices_label: list, timeseries_key: str ) -> dict:
        '''
        Fetch telemetry data from Thingsboard
        '''
        self.rest_client = RestClientPE(base_url=self.url)
        try:
            # Authenticate with credentials
            self.rest_client.login(username=self.username, password=self.password)

            # Get customer devices
            page = 0 
            all_devices = list()
            while True:
                devices = self.rest_client.get_customer_devices(
                    customer_id=self.customer_id, page_size=10, page=page)
                
                for device in devices.data:
                    all_devices.append(device)

                if devices.has_next == False:
                    break
                page += 1

            filtered_devices = self.__filter_devices(all_devices, devices_label)

            logging.info("Devices: \n%r", len(filtered_devices))


            telemetry_data = {}

            for device in filtered_devices:
                device_id = device.id.id
                device_details = self.rest_client.get_device_by_id(device_id)
                device_keys = timeseries_key

                logging.info("Getting telemetry")

                en = int(time.time())*1000
                st = int(time.time())*1000 - 24 * 60 * 60 * 1000

                print(st)
                print(en)
                telemetry = self.rest_client.telemetry_controller.get_timeseries_using_get(
                    "DEVICE",
                    device_id,
                    keys=device_keys,
                    start_ts = st,#int(time.time() * 1000) - 24 * 60 * 60 * 1000,
                    end_ts = en, #int(time.time() * 1000),
                )
                telemetry_data[device.label] = telemetry
            #print(telemetry)
            #save_telemetry_data(conn, telemetry_data)
            logging.info("Telemetry Saved")

        except ApiException as e:
            logging.exception(e)
        
        return telemetry_data

