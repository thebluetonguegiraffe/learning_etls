from concurrent.futures import ThreadPoolExecutor
from ETLs.thread import ThreadedETL
from common import is_prime


class TransformParallelizationETL(ThreadedETL):

    def __init__(self, batch_size, workers: int = 4):
        super().__init__(batch_size)
        self.workers = workers

    def transform(self, batch) -> list:
        with ThreadPoolExecutor() as executor:
            results = list(
                executor.map(
                    lambda num: {
                        "number": num,
                        "is_prime": is_prime(num**2 + 1),
                        "sqrt": sum(x**0.5 for x in range(1, 1000)),
                    },
                    batch,
                )
            )
        return results


class TransformerThreadParallelizationETL(ThreadedETL):

    def __init__(self, batch_size, workers: int = 4):
        super().__init__(batch_size)
        self.workers = workers

    def transformer_thread(self):
        with ThreadPoolExecutor() as executor:
            while True:
                batch = self.transformer_queue.get()

                if batch is None:
                    self.loader_queue.put(None)
                    break
                # print(f"Transforming batch: {batch[0]}-{batch[-1]}")
                future = executor.submit(self.transform, batch)
                transformed = future.result()

                self.loader_queue.put(transformed)

            self.loader_queue.put(None)  # Ensure loader knows when to stop


class AllTransformerParallelizationETL(ThreadedETL):

    def __init__(self, batch_size, workers: int = 4):
        super().__init__(batch_size)
        self.workers = workers

    def transform(self, batch) -> list:
        with ThreadPoolExecutor() as executor:
            results = list(
                executor.map(
                    lambda num: {
                        "number": num,
                        "is_prime": is_prime(num**2 + 1),
                        "sqrt": sum(x**0.5 for x in range(1, 1000)),
                    },
                    batch,
                )
            )
        return results

    def transformer_thread(self):
        with ThreadPoolExecutor() as executor:
            while True:
                batch = self.transformer_queue.get()

                if batch is None:
                    self.loader_queue.put(None)
                    break
                # print(f"Transforming batch: {batch[0]}-{batch[-1]}")
                future = executor.submit(self.transform, batch)
                transformed = future.result()

                self.loader_queue.put(transformed)

            self.loader_queue.put(None)  # Ensure loader knows when to stop


if __name__ == "__main__":
    etl = TransformParallelizationETL(batch_size=50)
    etl.run()

    etl = TransformerThreadParallelizationETL(batch_size=50)
    etl.run()

    etl = AllTransformerParallelizationETL(batch_size=50)
    etl.run()
