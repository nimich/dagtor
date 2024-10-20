class Client:
    def __init__(self):
        pass

    def connect(self):
        print("connected.")

    def register_pipeline(self, name) -> int:
        self.connect()  # open connector with state manage
        print("registered with id 0")
        registered_id = 0  # emulate registration with id -> 0
        return registered_id
