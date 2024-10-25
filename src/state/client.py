import psycopg


class Client:
    def __init__(self, connector_config):
        print(connector_config)
        try:
            self.conn = psycopg.connect(
                "host=localhost dbname=dagtor user=postgres port=5432 password=example"
            )
            print("Connection established")
        except psycopg.OperationalError as e:
            print(f"Failed to connect: {e}")
            self.conn = None

    def connect(self):
        if self.conn is not None:
            print("connect")

    def register_pipeline(self, name) -> int:
        self.connect()  # open connector with state manage
        print("registered with id 0")
        registered_id = 0  # emulate registration with id -> 0
        return registered_id
