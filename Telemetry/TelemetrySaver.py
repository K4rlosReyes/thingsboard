import sqlite3
import datetime


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
        self.ts_database_address = "ts_" + database_address
        self.ts_table_name = "ts_" + table_name
        self.ts_conn = sqlite3.connect(self.ts_database_address)
        self.ts_cursor = self.ts_conn.cursor()
        self.ts_cursor.execute(
            f"""
			 CREATE TABLE IF NOT EXISTS {self.ts_table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP
			 )
             """
        )

    def save_telemetry(self, telemetry_data: dict) -> None:
        # Create a database connection
        for device_name, telemetry in telemetry_data.items():
            print("--------------------------------------")
            for timeseries_key, ts_values in telemetry.items():
                for key_value in ts_values:
                    time_stamp = key_value["ts"]
                    value = key_value["value"]

                    self.cursor.execute(
                        f"INSERT INTO {self.table_name} (device_name, timestamp, key, value) VALUES (?, ?, ?, ?)",
                        (device_name, time_stamp, timeseries_key, value),
                    )
        self.conn.commit()

    def get_timestamp(self) -> str:
        current_timestamp = datetime.datetime.now()
        self.ts_cursor.execute(
            f"SELECT COALESCE(timestamp, ?) FROM {self.ts_table_name}",
            (current_timestamp,),
        )
        timestamp = self.ts_cursor.fetchone()
        return timestamp

    def update_timestamp(self) -> None:
        current_timestamp = datetime.datetime.now()

        # Update timestamp value
        self.ts_cursor.execute(
            f"UPDATE {self.ts_table_name} SET timestamp = ?", (current_timestamp,)
        )
        self.ts_connection.commit()

    def close(self) -> None:
        self.conn.close()
        self.ts_conn.close()
