import sqlite3


class TelemetrySaver(object):
    def __init__(self, database_address: str, table_name: str) -> None:
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

    def close(self) -> None:
        self.conn.close()
