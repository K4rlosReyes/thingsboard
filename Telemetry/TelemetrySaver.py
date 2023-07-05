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
                timestamp TIMESTAMP,
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
                timestamp TIMESTAMP
			 )
             """
        )

    def save_telemetry(self, telemetry_data: dict, timestamp: int) -> None:
        # Create a database connection
        for device_name, telemetry in telemetry_data.items():
            # print("--------------------------------------")
            for timeseries_key, ts_values in telemetry.items():
                for key_value in ts_values:
                    time_stamp = key_value["ts"]
                    value = key_value["value"]

                    self.cursor.execute(
                        f"INSERT INTO {self.table_name} (device_name, timestamp, key, value) VALUES (?, ?, ?, ?)",
                        (device_name, time_stamp, timeseries_key, value),
                    )

        # Update timestamp value
        self.cursor.execute(
            f"UPDATE {self.ts_table_name} SET timestamp = ?", (timestamp,)
        )
        self.conn.commit()

    def get_timestamp(self) -> int:
        current_timestamp = int(time.time()) * 1000
        self.cursor.execute(f"SELECT timestamp FROM {self.ts_table_name}")
        timestamp = self.cursor.fetchone()

        if timestamp is not None:
            timestamp_int = int(timestamp[0])
            print(timestamp_int)
            return timestamp_int
        else:
            # Si no se encontró ningún resultado, se puede devolver el valor actual
            return current_timestamp

    def close(self) -> None:
        self.conn.close()
