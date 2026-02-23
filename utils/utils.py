import json

from utils.logger_utils import get_logger

_LOGGER = get_logger(__name__)


def get_hot_wallets(chain_id: str) -> list[str]:
    with open("artifacts/centralized_exchange_addresses.json") as f:
        centralized_exchanges = json.load(f)

    hot_wallets: list[str] = list()
    for exchange_id, info in centralized_exchanges.items():
        _exchange_hot_wallets = info.get("wallets", {})
        hot_wallets.extend([w.lower() for w in _exchange_hot_wallets.get(chain_id, [])])
    return list(set(hot_wallets))


def get_burn_wallets(chain_id: str) -> list[str]:
    with open("artifacts/burn_wallets.json") as f:
        burn_wallets_data = json.load(f)
    return [wallet.lower() for wallet in burn_wallets_data.get(chain_id, [])]


def to_normalized_address(address: str | None) -> str | None:
    if address is None:
        return address
    return address.lower()


# def calculate_time(func):
#     def wrapper(*args, **kwargs):
#         start_time = time.time()
#         returned_value = func(*args, **kwargs)
#         end_time = time.time()
#         _LOGGER.debug(f"Processing time: {end_time - start_time}")
#         return returned_value

#     return wrapper


# def aggregate_separated_logs(
#     current_merged_logs: dict, adding_logs: dict, chain_id=None
# ):
#     merged_logs = copy.deepcopy(current_merged_logs)
#     for adding_key, adding_value in adding_logs.items():
#         key = f"{chain_id}_{adding_key}" if chain_id is not None else adding_key
#         if adding_key in merged_logs.keys():
#             merged_logs[key] += adding_value
#         else:
#             merged_logs[key] = adding_value
#     return merged_logs


# def update_token_change_logs(current_merged_logs: dict, adding_logs: dict):
#     merged_logs = copy.deepcopy(current_merged_logs)
#     for token, log in adding_logs.items():
#         if token not in merged_logs.keys():
#             merged_logs[token] = log
#         else:
#             merged_log = merged_logs[token]
#             for timestamp, value in log.items():
#                 if merged_log.get(timestamp) is None:
#                     merged_log[timestamp] = value
#                 else:
#                     merged_log[timestamp].update(value)

#     return merged_logs


# def concat_chain_id(token_dict: dict, chain_id):
#     concat_dict = {}
#     for token_address, value in token_dict.items():
#         concat_dict[f"{chain_id}_{token_address}"] = value
#     return concat_dict


# def change_logs_integer_timestamp(change_logs):
#     return {int(t): v for t, v in change_logs.items()}


# def token_change_logs_integer_timestamp(token_change_logs):
#     result = {}
#     for token, change_logs in token_change_logs.items():
#         result[token] = change_logs_integer_timestamp(change_logs)
#     return result


# def add_prefix_to_key_of_dict(dict_object: dict, prefix):
#     new_dict = dict()
#     for k in dict_object:
#         new_dict[f"{prefix}_{k}"] = dict_object[k]
#     return new_dict
