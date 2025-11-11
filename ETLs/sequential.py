import time

from common import is_prime


class SequentialETL:
    def __init__(self, batch_size: int = 50):
        self.batch_size = batch_size
        self.start_batch = 0

        self.database = []  # Simula una DB

    def extract(self) -> list:
        batches = []
        for i in range(0, 1000, self.batch_size):
            time.sleep(0.1)  # Simula latencia de red
            batch = list(range(i + 1, min(i + self.batch_size + 1, 1001)))
            batches.append(batch)
            print(f"  Extracted batch: {batch[0]}-{batch[-1]}")
        return batches

    def transform(self, batch) -> list:
        print(f"  Transforming batch: {batch[0]}-{batch[-1]}")
        transformed = []
        for num in batch:
            transformed.append({
                "number": num,
                "is_prime": is_prime(num**2 + 1),
                "sqrt": sum(x**0.5 for x in range(1, 1000)),
            })
        return transformed

    def load(self, batch):
        print(f"  Loading batch: {batch[0]['number']}-{batch[-1]['number']}")
        time.sleep(0.05)  # Simula escritura a DB
        self.database.extend(batch)

    def run(self):
        start = time.time()

        # variables flow over the steps
        batches = self.extract()

        transformed_batches = []
        for batch in batches:
            transformed_batches.append(self.transform(batch))

        for batch in transformed_batches:
            self.load(batch)

        elapsed = time.time() - start
        print(f"\n✅ Completed! Total records: {len(self.database)}")
        print(f"⏱️  Time: {elapsed:.2f}s")
        return elapsed


if __name__ == "__main__":
    etl = SequentialETL(batch_size=50)
    etl.run()
