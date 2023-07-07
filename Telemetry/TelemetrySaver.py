import sqlite3
import datetime
import time


class TelemetrySaver(object):
    def __init__(self, database_address: str, table_name: str) -> None:
        # Main Database Info
        self.database_address = database_address
        self.table_name = table_name
        self.conn = sqlite3.connect(self.database_address)
        self.cursor = self.conn.cursor()
        self.cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
				device_name TEXT, 
                timestamp INT,
				key TEXT,
                value FLOAT
            )
        """
        )

        # Timestamp Database Info
        self.ts_table_name = "ts_" + table_name
        self.cursor.execute(
            f"""
			 CREATE TABLE IF NOT EXISTS {self.ts_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INT
			 )
             """
        )

    def save_telemetry(self, telemetry_data: dict, timestamp: int) -> None:
        """
        Save telemetry stored on telemetry_data and update last timestamp.
        """
        
        # Take the telemetry info for each device
        for device_name, telemetry in telemetry_data.items():
            
            # Take one by one each time serie. 
            for timeseries_key, ts_values in telemetry.items():

                # For each value in time series
                for key_value in ts_values:
                    time_stamp = key_value["ts"]
                    value = key_value["value"]
                    # insert an entry in the table 
                    self.cursor.execute(
                        f"INSERT INTO {self.table_name} (device_name, timestamp, key, value) VALUES (?, ?, ?, ?)",
                        (device_name, time_stamp, timeseries_key, value),
                    )

        # Update last timestamp value
        self.cursor.execute(
            f"UPDATE {self.ts_table_name} SET timestamp = ? WHERE id = 1", (timestamp,)
        )
        self.conn.commit()

    def get_last_timestamp(self) -> int:
        """
        Return timestamp for the last Telemetry save
        """

        # Default value for timestamp
        last_timestamp = int(time.time()) * 1000 - 24 * 60 * 60 * 1000
        
        # get last timestamp stored in db
        self.cursor.execute(f"SELECT timestamp FROM {self.ts_table_name} WHERE id = 1")
        timestamp = self.cursor.fetchone()

        if timestamp is not None:
            last_timestamp = int(timestamp[0])
        else:
            # Si no se encontró ningún valor se agrega uno con el valor por default
            self.cursor.execute(
                f"INSERT INTO {self.ts_table_name} (id, timestamp) VALUES (1, ?)",
                (last_timestamp,),
            )

        return last_timestamp

    def close(self) -> None:
        """
        Close SQLite connection
        """
        self.conn.close()
