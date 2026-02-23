import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(sys.path[0])))

import json

from databases.arangodb_klg import ArangoDB
from databases.mongodb_label import MongoDBLabel
from utils.logger_utils import get_logger

logger = get_logger("Bot Detection")

"""
1. Send money to too many wallet
2. Not power law distribution of time between
"""


def check_bot(chain_id, threshold=500, batch_size=50):
    if chain_id == "0x1":
        chain_name = "ethereum"
        provider = "https://rpc.ankr.com/eth"
    elif chain_id == "0x38":
        chain_name = "bsc"
        provider = "https://bsc-dataseed3.bnbchain.org"
    else:
        raise ValueError("Chain Id must be 0x38 or 0x1")

    mongo_label = MongoDBLabel()
    arango = ArangoDB(prefix=chain_name)
    # _query = f"""
    #     FOR w in {chain_name}_addresses
    #     FILTER w.numberSent > {threshold}
    #     RETURN w.address
    # """
    #
    # bots: list[str] = list()
    #
    # _cursor = arango._db.aql.execute(_query, batch_size=batch_size, count=True)
    #
    # print(f'Number of addresses that sent to more than {threshold} addresses on {chain_id}: {len(_cursor)}')
    #
    # _count = 0
    # while True:
    #     addresses_batch = list(_cursor.batch())
    #     _cursor.batch().clear()
    #     for addr, is_contract in check_if_contracts(addresses_batch, provider_url=provider).items():
    #         if not is_contract:
    #             bots.append(addr)
    #
    #     _count += 1
    #     print(f'To {_count * batch_size} / {len(_cursor)}')
    #     if _cursor.has_more():
    #         _cursor.fetch()
    #     else:
    #         break
    #
    # print(f"Number of bots: {len(bots)}")
    # with open(f'../../.data/bots_{chain_id}.json', 'w') as f:
    #     json.dump(bots, f, indent=2)

    with open(f"../../.data/bots_{chain_id}.json", "r") as f:
        bots = json.load(f)

    # Update
    # _update_query = f"""
    #     FOR addr in {bots}
    #         UPDATE {{
    #             _key: CONCAT('{chain_id}_', addr),
    #             wallet: {{'bot': 1}}
    #         }} in {chain_name}_addresses
    # """
    # arango._db.aql.execute(_update_query)

    mongo_label.insert_bots(
        data=[{"address": bot, "chainId": chain_id} for bot in bots]
    )


# if __name__ == '__main__':
#     check_bot('0x1')
