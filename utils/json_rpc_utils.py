import json
from collections.abc import Iterable

from typing_extensions import override
from web3 import HTTPProvider
from web3.types import RPCEndpoint, RPCResponse


def _generate_get_code_json_rpc(
    contract_addresses: Iterable[str], block: str = "latest"
) -> Iterable[tuple[RPCEndpoint, dict[str, str | list[str] | int]]]:
    """
    Generate JSON-RPC requests to get the bytecode of contract addresses.
    """
    for idx, contract_address in enumerate(contract_addresses):
        yield _generate_json_rpc(
            method="eth_getCode",
            params=[contract_address, hex(block) if isinstance(block, int) else block],
            request_id=idx,
        )


def _generate_json_rpc(
    method: str, params: list[str], request_id: int = 1
) -> tuple[RPCEndpoint, dict[str, str | list[str] | int]]:
    json_rpc_request: dict[str, str | list[str] | int] = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": request_id,
    }
    return RPCEndpoint(method), json_rpc_request


class _BatchHTTPProvider(HTTPProvider):
    @override
    def make_batch_request(
        self, batch_requests: list[tuple[RPCEndpoint, dict[str, str | list[str] | int]]]
    ) -> list[RPCResponse]:
        """
        Make a batch HTTP request to the blockchain node.
        """

        batch_requests_str: str = json.dumps(batch_requests)
        self.logger.debug(
            "Making request HTTP. URI: %s, Request: %s",
            self.endpoint_uri,
            batch_requests_str,
        )
        request_data: bytes = batch_requests_str.encode("utf-8")
        kwargs = self.get_request_kwargs()

        if not all(isinstance(key, str) for key in kwargs.keys()):
            raise TypeError("All keys in the dictionary must be strings.")
        raw_response: bytes = self._request_session_manager.make_post_request(
            self.endpoint_uri, request_data, **kwargs
        )
        response = self.decode_rpc_response(raw_response)
        if not isinstance(response, list):
            # RPC errors return only one response with the error object
            raise ValueError(
                "Batch request did not return a list of responses. Check if the provider supports batch requests."
            )
        self.logger.debug(
            "Getting response HTTP. URI: %s, Request: %s, Response: %s",
            self.endpoint_uri,
            batch_requests,
            response,
        )
        return response


def _rpc_response_to_result(response: RPCResponse) -> str:
    """
    Extract the result field from a JSON-RPC response.
    """
    try:
        result: str = response.get("result", "")
        return result
    except Exception as e:
        print(f"Error extracting result from response: {e}")
        return ""


def check_if_contracts(
    addresses: list[str], provider_url: str = "https://bsc-dataseed3.ninicoin.io/"
) -> dict[str, bool]:
    """
    Check if a list of addresses are contracts by querying the blockchain node.
    Args:
        addresses (list[str]): list of addresses to check.
        provider_url (str): URL of the blockchain node.
    Returns:
        dict[str, bool]: A dictionary mapping addresses to a boolean indicating if they are contracts.
    """
    returned_dict: dict[str, bool] = {addr.lower(): True for addr in addresses}
    json_rpc: list[tuple[RPCEndpoint, dict[str, str | list[str] | int]]] = list(
        _generate_get_code_json_rpc(returned_dict.keys())
    )

    _batch_provider: _BatchHTTPProvider = _BatchHTTPProvider(provider_url)
    response_batch: list[RPCResponse] = _batch_provider.make_batch_request(json_rpc)

    for response in response_batch:
        if "id" not in response:
            raise ValueError("Response does not contain a valid request ID.")
        else:
            request_id = response[
                "id"
            ]  # request id is the index of the contract in contracts list
        if not isinstance(request_id, int):
            raise TypeError(
                f"Request ID must be an integer, got {type(request_id).__name__}"
            )
        if not response.get("result"):
            returned_dict[addresses[request_id]] = False
        bytecode: str = _rpc_response_to_result(response)
        if bytecode == "0x":
            returned_dict[addresses[request_id]] = False

    return returned_dict
