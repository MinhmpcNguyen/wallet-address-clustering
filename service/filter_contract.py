import pandas as pd
from multithread_processing.base_job import BaseJob

from utils.json_rpc_utils import check_if_contracts


class DetectContract(BaseJob):
    """
    A job to detect whether a list of addresses are contracts and save the results.
    """

    def __init__(
        self,
        output_path: str,
        list_index: list[str],
        provider_url: str,
        max_workers: int = 4,
        batch_size: int = 1000,
    ):
        """
        Args:
            output_path (str): Path to save the output CSV file.
            list_index (list): List of addresses to check.
            provider_url (str): URL of the blockchain node.
            max_workers (int): Maximum number of workers for multithreading.
            batch_size (int): Batch size for processing.
        """
        self.is_contract = dict()
        self.output = output_path
        self.provider_url = provider_url

        super().__init__(
            work_iterable=list_index, max_workers=max_workers, batch_size=batch_size
        )

    def _execute_batch(self, works):
        """
        Process a batch of addresses to check if they are contracts.
        """
        try:
            contract = check_if_contracts(
                addresses=works, provider_url=self.provider_url
            )
        except Exception as e:
            print(f"Error processing batch: {e}")
        else:
            self.is_contract.update(contract)

    def _end(self):
        """
        Finalize the job and save the results to a CSV file.
        """
        super()._end()
        df = pd.DataFrame(
            list(self.is_contract.items()), columns=["address", "IsContract"]
        )
        df.to_csv(self.output, index=False)
