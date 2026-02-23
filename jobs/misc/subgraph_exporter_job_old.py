# from typing import Dict, Set, List
# from multithread_processing.base_job import BaseJob
# import pandas as pd

# import os
# import sys
# sys.path.append(os.path.dirname(sys.path[0]))

# from constants.arangodb_constants import ArangoDBPrefix
# from databases.arangodb_klg import ArangoDB
# from databases.mongodb import MongoDB
# from utils.logger_utils import get_logger

# LIMIT_NUMBER_OF_EDGES = 10000
# logger = get_logger('Subgraphs Loader (to Mongo) Job')


# class SubgraphExporterJob(BaseJob):
#     """Job to fill transfer & transaction graph
#     """
#     def __init__(self,
#                  importer: ArangoDB,
#                  exporter: MongoDB,
#                  chain_id: str,
#                  addresses: Set,
#                  radius: int = 2,
#                  batch_size=100, max_workers=8,
#                  ):
#         """
#         Args:
#             self: Represent the instance of the class
#             chain_id: Specify the chain that we want to extract data from
#             batch_size: Set the number of work for each worker to process parallely
#             max_workers: Limit the number of workers that can be used to process the work_iterable
#             sources: Specify the source of the data
#     """

#         self.exporter = exporter
#         self.importer = importer

#         self.chain_id = chain_id
#         self.radius = radius

#         self.work_iterable = list(addresses)
#         self._work_iterable_tracking: Dict = {addr: i for i, addr in enumerate(self.work_iterable)}
#         self.batch_size = batch_size
#         self.max_workers = max_workers
#         super().__init__(work_iterable=self.work_iterable,
#                          batch_size=batch_size,
#                          max_workers=max_workers)

#     def _start(self):
#         logger.info("Start exporting subgraphs from Arango to Mongo")

#     def _execute_batch(self, works):
#         subgraphs_list: List[Dict] = list()
#         for address in works:
#             try:
#                 subgraph_cursor = self.importer.get_subgraph(address=address, depth=self.radius)
#                 subgraph_edges = [
#                     {'from': edge['from'].split('_')[-1],
#                      'to': edge['to'].split('_')[-1]}
#                      for edge in subgraph_cursor
#                 ]
#                 if subgraph_edges and len(subgraph_edges) < LIMIT_NUMBER_OF_EDGES:
#                     subgraphs_list.append({
#                         'address': address,
#                         'chainId': self.chain_id,
#                         'edges': subgraph_edges,
#                     })
#             except Exception as ex:
#                 logger.info(ex)
#         self._export_subgraphs(subgraphs=subgraphs_list)
#         logger.info(f"Exported subgraphs of addresses {self._work_iterable_tracking[works[0]]} "
#                     f"to {self._work_iterable_tracking[works[-1]]} / {len(self.work_iterable)}")

#     def _end(self):
#         self.batch_executor.shutdown()

#     def _export_subgraphs(self, subgraphs: List[Dict]):
#         self.exporter.update_subgraphs(data=subgraphs, chain_name='ethereum_ens',radius=2)


# if __name__ == '__main__':
#     # for the ens
#     WALLETS_PAIRS_PATH = '../data/pairs_data/ethereum_ens_pairs.csv'

#     chain_id = '0x1'
#     wallets_pairs_df = pd.read_csv(WALLETS_PAIRS_PATH)
#     addresses = wallets_pairs_df.from_address.unique().tolist()
#     job = SubgraphExporterJob(
#         importer=ArangoDB(prefix='ethereum'),
#         exporter=MongoDB(),
#         chain_id='0x1',
#         addresses=addresses,
#         radius=2,
#         batch_size=1000,
#         max_workers=6,
#     )
#     job.run()
