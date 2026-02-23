import json
from typing import Dict, Iterable, List, Union

import requests

JsonPrimitive = Union[str, int, float, bool, None]
BlockParam = Union[int, str]  # int block number or "latest", "safe", "finalized", etc.


def _to_hex_block_param(block: BlockParam) -> str:
    """Convert int block -> 0x... hex string; keep string params as-is."""
    if isinstance(block, int):
        return hex(block)
    return block  # "latest" / "safe" / "finalized" / etc.


def _generate_json_rpc_batch(
    method: str, params_list: Iterable[List[JsonPrimitive]], start_id: int = 0
) -> List[Dict[str, JsonPrimitive]]:
    """Build a batch (list) of JSON-RPC 2.0 requests with incremental ids."""
    batch = []
    for i, params in enumerate(params_list, start=start_id):
        batch.append(
            {
                "jsonrpc": "2.0",
                "id": i,
                "method": method,
                "params": params,
            }
        )
    return batch


def _post_json_rpc_batch(
    endpoint: str,
    batch_payload: List[Dict[str, JsonPrimitive]],
    timeout: float = 30.0,
) -> List[Dict[str, JsonPrimitive]]:
    """POST a JSON-RPC batch; return parsed JSON list. Raises for bad HTTP."""
    headers = {"Content-Type": "application/json"}
    resp = requests.post(
        endpoint, data=json.dumps(batch_payload), headers=headers, timeout=timeout
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError(f"Expected list batch response, got: {type(data)} - {data}")
    return data


def check_if_contracts(
    addresses: List[str],
    provider_url: str,
    block: BlockParam = "latest",
    batch_size: int = 1000,
    timeout: float = 30.0,
) -> Dict[str, bool]:
    """
    Check if each address has bytecode at `block` using eth_getCode in JSON-RPC batch mode.

    Returns:
        dict[address_lower] -> bool  (True = is contract (has code), False = EOA / not found)

    Notes:
    - Works with Web3 v6+ (no internal imports).
    - Uses `requests` to batch-call JSON-RPC endpoint directly for maximum compatibility.
    - `block` may be an int (converted to hex) or a string like "latest".
    - Large inputs are split into batches to avoid oversized payloads.
    """
    # Normalize addresses and keep an index map for response id correlation
    addr_list = [addr.lower() for addr in addresses]
    results: Dict[str, bool] = {addr: False for addr in addr_list}
    hex_block = _to_hex_block_param(block)

    # Process in batches
    request_id_base = 0
    for start in range(0, len(addr_list), batch_size):
        chunk = addr_list[start : start + batch_size]

        # Build params list: [address, blockTag]
        params_list = [[addr, hex_block] for addr in chunk]

        # Build batch with incremental ids tied to the original global index
        batch = _generate_json_rpc_batch(
            method="eth_getCode", params_list=params_list, start_id=request_id_base
        )
        # Map request id back to address
        id_to_addr = {request_id_base + i: addr for i, addr in enumerate(chunk)}

        # Send batch
        try:
            resp_items = _post_json_rpc_batch(provider_url, batch, timeout=timeout)
        except requests.RequestException as e:
            # Network/HTTP error: leave this batch as default False, but surface a clearer error
            raise RuntimeError(
                f"RPC batch request failed for {provider_url}: {e}"
            ) from e

        # Parse responses
        for item in resp_items:
            rid = item.get("id")
            if rid is None or rid not in id_to_addr:
                # Skip unknown/malformed response items
                continue

            addr = id_to_addr[rid]
            if "error" in item:
                # If RPC returned an error for this item, treat as no-code (EOA) but you may log it if needed
                # print(f"RPC error for {addr}: {item['error']}")
                results[addr] = False
                continue

            code = item.get("result")
            # According to JSON-RPC, empty code is "0x"
            results[addr] = bool(code and code != "0x")

        request_id_base += len(chunk)

    # Return only for input addresses (lowercased keys)
    return {addr: results[addr.lower()] for addr in addresses}
