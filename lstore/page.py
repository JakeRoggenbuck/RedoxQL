
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        pass

    def write(self, value):
        self.num_records += 1
        pass

