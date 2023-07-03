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
                timestamp TEXT,
				key TEXT,
                value TEXT
            )
        """
        )


	def save_telemetry( self, telemetry_data: dict ) -> None:
		# Create a database connection
		for device_name, telemetry in telemetry_data.items():
			print("--------------------------------------")
			for timeseries_key, ts_values in telemetry.items():

				# TODO: Cambiar el timestamp al timestamp de envío de información de los dispositivos
				for key_value in ts_values:
					time_stamp = key_value["ts"]
					value = key_value["value"]
					#timestamp = datetime.fromtimestamp(int(time_stamp) // 1000).strftime(
					#	"%Y-%m-%d %H:%M:%S"
					#)

					self.cursor.execute(
						f"INSERT INTO {self.table_name} (device_name, timestamp, key, value) VALUES (?, ?, ?, ?)",
						(device_name, time_stamp, timeseries_key, value),
					)
		self.conn.commit()

		self.conn.close()
	
	def close(self) ->None:
		self.conn.close()

