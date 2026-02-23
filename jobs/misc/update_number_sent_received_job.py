import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(sys.path[0])))
import time

from multithread_processing.base_job import BaseJob

from databases.arangodb_klg import ArangoDB
from utils.logger_utils import get_logger

ARANGO_BATCH_SIZE = 50000
logger = get_logger("Get number Sent & Received of addresses")


class UpdateNumberSentReceivedJob(BaseJob):
    def __init__(
        self,
        addresses: list[str],
        # chain_name: str,
        chain_id: str,
        arango: ArangoDB,
        batch_size: int = 10000,
        max_workers: int = 8,
    ):
        keys = [f"{chain_id}_{addr}" for addr in addresses]
        super().__init__(keys, batch_size, max_workers)
        self.arango = arango
        self.addresses_with_number_sent_received: list[dict] = []

    def _execute_batch(self, works):
        _time = int(time.time())
        _query = f"""
        FOR k in {works}
            LET id = CONCAT('{self.arango.prefix}_addresses/', k)
            LET n_to = (FOR v IN 1..1 OUTBOUND id
                GRAPH {self.arango.prefix}_transfers_graph
                COLLECT WITH COUNT INTO n
                RETURN n)
            LET n_from = (FOR v IN 1..1 INBOUND id
                GRAPH {self.arango.prefix}_transfers_graph
                COLLECT WITH COUNT INTO n
                RETURN n)
            UPDATE {{_key: k, numberSent: n_to[0], numberReceived: n_from[0], lastUpdatedAt:{_time} }}
                IN {self.arango.prefix}_addresses
            LET updated = NEW
            RETURN {{
                '_key': updated._key,
                'address': updated.address,
                'numberSent': updated.numberSent,
                'numberReceived': updated.numberReceived
            }}
        """
        new_data = self.arango.query(_query, batch_size=ARANGO_BATCH_SIZE)
        self.addresses_with_number_sent_received.extend(list(new_data))

    def run(self) -> list[dict]:
        super().run()
        return self.addresses_with_number_sent_received


# if __name__ == "__main__":
#     _chain_name = input(
#         "Name of the chain to get numberSent/numberReceived (bsc or ethereum): "
#     )
#     # _chain_name = 'ethereum'
#     arango = ArangoDB(prefix=_chain_name)
#     arango_batch_size = 1000

#     """Only calculate numberSent/numberReceived for deposit wallets"""
#     while True:
#         try:
#             cursor = arango._db.aql.execute(
#                 # query=f"""FOR w IN {_chain_name}_addresses
#                 #           FILTER w.numberSent == null
#                 #           RETURN w""",
#                 query=f"""FOR w IN {_chain_name}_addresses
#                           FILTER v.wallet.depositWallet
#                           RETURN w""",
#                 batch_size=arango_batch_size,
#                 count=True,
#             )

#             n_addresses = list(arango.query(f"RETURN LENGTH({_chain_name}_addresses)"))[
#                 0
#             ]
#             logger.info(f"Number of addresses to process: {n_addresses}")
#             _count = 0
#             while True:
#                 _data = list(cursor.batch())
#                 _count += 1
#                 cursor.batch().clear()
#                 _addr_keys = [_datum["_key"] for _datum in _data]
#                 job = UpdateNumberSentReceived(
#                     address_keys=_addr_keys,
#                     chain_name=_chain_name,
#                     arango=arango,
#                     batch_size=1000,
#                     max_workers=4,
#                 )
#                 job.run()
#                 logger.info(f"To {_count * arango_batch_size} / {n_addresses}")
#                 if cursor.has_more():
#                     cursor.fetch()
#                 else:
#                     break
#             logger.info("Finished get numberSent and numberReceived of all wallets")
#             break
#         except AQLQueryExecuteError as ex:
#             logger.exception(ex)
#             if ex.args[0].endswith("timeout in cluster operation (while executing)"):
#                 logger.info("Sleep 5 seconds then retry")
#                 time.sleep(5)
#             else:
#                 break
