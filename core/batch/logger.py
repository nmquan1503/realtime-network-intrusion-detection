import time

class Logger:
    def __init__(self, name="PIPELINE"):
        self.name = name
        self.start_time = None
        self.last_time = None

    def start(self):
        self.start_time = time.time()
        self.last_time = self.start_time
        print(f"\n========== {self.name} START ==========\n")

    def log(self, msg: str):
        now = time.time()
        elapsed = now - self.last_time
        total = now - self.start_time

        print(f"[{msg}] +{elapsed:.2f}s | total {total:.2f}s")

        self.last_time = now

    def end(self):
        total = time.time() - self.start_time
        print(f"\n========== DONE ==========")
        print(f"[TOTAL TIME] {total:.2f}s\n")