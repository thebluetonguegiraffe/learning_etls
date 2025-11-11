from concurrent.futures import ThreadPoolExecutor
from ETLs.transform_parallelization import AllTransformerParallelizationETL


class LoadParallelizationETL(AllTransformerParallelizationETL):
    """
    Paralelizar el loader usando un ThreadPoolExecutor no tiene sentido puesto que list.extend() es
    una operación muy rápida y atómica en Python
    """

    pass


class LoaderThreadParallelizationETL(AllTransformerParallelizationETL):

    def __init__(self, batch_size, workers: int = 4):
        super().__init__(batch_size)
        self.workers = workers

    def loader_thread(self):
        with ThreadPoolExecutor() as executor:
            while True:
                batch = self.loader_queue.get()

                if batch is None:
                    break
                executor.submit(self.load, batch)
                # print(f"Loading batch: {batch[0]['number']}-{batch[-1]['number']}")


if __name__ == "__main__":
    etl = LoaderThreadParallelizationETL(batch_size=50)
    etl.run()
