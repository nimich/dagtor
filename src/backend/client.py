class Client:
    def __init__(self):
        pass

    def connect(self):
        print("connected.")

    def register_pipeline(self, name) -> int:
        self.connect()
        print("registered with id 0")
        return 0
