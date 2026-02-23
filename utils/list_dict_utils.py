from collections.abc import Iterator
from typing import Any

from schemas.mongo_db_schema import DepositUserDoc

# def to_change_logs(d: dict):
#     return {int(t): v for t, v in d.items()}


# def sorted_dict(d: dict, reverse=False):
#     return dict(sorted(d.items(), key=lambda x: x[0], reverse=reverse))


# def sort_log(log):
#     log = to_change_logs(log)
#     log = sorted_dict(log)
#     return log


# def sort_log_dict(log_dict):
#     for key in log_dict:
#         log = log_dict[key]
#         log_dict[key] = sort_log(log)
#     return log_dict


# def cut_change_logs(
#     change_logs: dict,
#     end_time: int = None,
#     start_time: int = None,
#     duration: int = None,
#     alt_value=None,
# ):
#     if not end_time:
#         end_time = int(time.time())

#     if not start_time:
#         if not duration:
#             raise ValueError("start_time or duration must be set")
#         else:
#             start_time = end_time - duration

#     change_logs = to_change_logs(change_logs)
#     change_logs = sorted_dict(change_logs)
#     for t in change_logs.keys():
#         if (t < start_time) or (t > end_time):
#             change_logs[t] = alt_value

#     return change_logs


def chunks(input_list: list[str], size: int) -> Iterator[list[str]]:
    for i in range(0, len(input_list), size):
        yield input_list[i : i + size]


# def chunks_dict(data: dict, size=50):
#     it = iter(data)
#     for i in range(0, len(data), size):
#         yield {k: data[k] for k in islice(it, size)}


# def prune_change_logs(
#     value,
#     change_logs=None,
#     duration=TimeConstants.DAYS_31,
#     interval=TimeConstants.A_DAY,
# ):
#     if change_logs is None:
#         change_logs = {}
#     else:
#         change_logs = sort_log(change_logs)

#     current_time = int(time.time())
#     out_date_time = current_time - duration

#     change_logs[current_time] = value
#     batch = {}
#     for k in change_logs:
#         if k < out_date_time:
#             change_logs[k] = None
#         else:
#             batch_idx = k // interval
#             if batch.get(batch_idx):
#                 change_logs[k] = None
#             else:
#                 batch[batch_idx] = True
#     return change_logs


# def combined_logs(*logs, handler_func=sum, default_value=0):
#     timestamps = set()
#     for log in logs:
#         timestamps.update(list(log.keys()))
#     timestamps = sorted(timestamps)

#     combined = {}
#     current_values = [default_value] * len(logs)
#     for t in timestamps:
#         for idx, log in enumerate(logs):
#             current_values[idx] = log.get(t, current_values[idx])

#         combined[t] = handler_func(current_values)

#     return combined


# def combined_token_change_logs_func(values):
#     value_in_usd = 0
#     for value in values:
#         if value is not None:
#             value_in_usd += value["valueInUSD"]
#     return value_in_usd


def flatten_dict(d: DepositUserDoc | dict) -> dict[Any, Any]:  # pyright: ignore [reportUnknownParameterType,reportMissingTypeArgument]
    out = {}
    for key, val in d.items():  # pyright: ignore [reportMissingTypeStubs]
        if isinstance(val, dict):
            val = [val]
        if isinstance(val, list):
            array = []
            for subdict in val:
                if not isinstance(subdict, dict):
                    array.append(subdict)
                else:
                    deeper = flatten_dict(subdict).items()
                    _ = out.update(
                        {str(key) + "." + str(key2): val2 for key2, val2 in deeper}
                    )
            if array:
                _ = out.update({str(key): array})
        else:
            out[str(key)] = val

    return out


def delete_none(_dict: DepositUserDoc | dict) -> DepositUserDoc | dict:  # pyright: ignore [reportUnknownParameterType,reportMissingTypeArgument]
    """Delete None values recursively from all the dictionaries"""
    for key, value in list(_dict.items()):
        if isinstance(value, dict):
            _ = delete_none(value)
        elif value is None:
            del _dict[key]
        elif isinstance(value, list):
            for v_i in value:
                if isinstance(v_i, dict):
                    delete_none(v_i)

    return _dict
