from typing import Literal


class ContractConst:
    decimals: str = "decimals"
    protocol_data_address: str = "protocol_data_address"
    comptroller_address: str = "comptroller_address"
    new_comptroller_address: str = "new_comptroller_address"
    comptroller_implementation_address: str = "comptroller_implementation_address"
    chef_incentive_address: str = "chef_incentive_address"
    multi_fee_address: str = "multi_fee_address"
    oracle_address: str = "oracle_address"
    chainlink_address: str = "chainlink_address"
    coin_base_address: str = "coin_base_address"
    staked_incentive_address: tuple[Literal["staked_incentive_address"]] = (
        "staked_incentive_address",
    )
    staked_token: str = "staked_token"
    lending_abi: str = "lending_abi"
    lending_fork: str = "lending_fork"
    comptroller_abi: str = "comptroller_abi"
    chain_link_abi: str = "chain_link_abi"
    oracle_abi: str = "oracle_abi"
    token_abi: str = "token_abi"
    multi_fee_abi: str = "multi_fee_abi"
    chef_incentive_abi: str = "chef_incentive_abi"
    protocol_data_abi: str = "protocol_data_abi"
    speed_function: str = "speed_function"
    metadata_tokens: str = "metadata_tokens"
    dapp_name: str = "dapp_name"
    name: str = "name"
    chain_id: str = "chain_id"
    chain_name: str = "chain_name"
    lending_address: str = "lending_address"
    mint_event: str = "Mint"
    deposit_event: str = "Deposit"
    borrow_event: str = "Borrow"
    token: str = "token"
    img_url: str = "img_url"
