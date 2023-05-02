import mysql.connector

class BatchProcessor:
    def __init__(self, host, username, password, database):
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        
    def batch_insert(self, table, rows):
        """Batch insert rows into table."""
        try:
            conn = mysql.connector.connect(
                host=self.host,
                user=self.username,
                password=self.password,
                database=self.database
            )
            cursor = conn.cursor()
            placeholders = ', '.join(['%s'] * len(rows[0]))
            query = f"INSERT INTO {table} VALUES ({placeholders})"
            cursor.executemany(query, rows)
            conn.commit()
            print(f"{len(rows)} rows inserted into {table} table.")
        except mysql.connector.Error as error:
            print(f"Error while inserting rows into {table} table: {error}")
        finally:
            cursor.close()
            conn.close()

