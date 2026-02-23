from models.graph.transfer import Transfer


class Edge:
    def __init__(
        self,
        chain_id: str,
        from_address: str,
        to_address: str,
        transfer_logs: dict[str, dict[int, dict[str, float | None]]] | None = None,
        oldest_transfer_at: int | None = None,
    ):
        """Relationship between 2 address"""
        self.chain_id: str = chain_id
        self.from_address: str = from_address
        self.to_address: str = to_address
        self.key: str = f"{self.chain_id}_{self.from_address}_{self.to_address}"

        if not transfer_logs:
            self._transfer_logs: dict[str, dict[int, dict[str, float | None]]] = dict()
        else:
            self._transfer_logs = transfer_logs

        self.oldest_transfer_at: int | None = oldest_transfer_at

    def add_transfer(
        self, transfer: Transfer, value_in_usd: float | None
    ):  # TODO: rename to "add_transfer"
        if transfer.coin_addr in self._transfer_logs:
            self._transfer_logs[transfer.coin_addr].update(
                {
                    transfer.timestamp: {
                        "amount": transfer.amount,
                        "valueInUSD": value_in_usd,
                    }
                }
            )
        else:
            self._transfer_logs.update(
                {
                    transfer.coin_addr: {
                        transfer.timestamp: {
                            "amount": transfer.amount,
                            "valueInUSD": value_in_usd,
                        }
                    }
                }
            )

    def get_transfer_logs(self) -> dict[str, dict[int, dict[str, float | None]]]:
        return self._transfer_logs

    def prune_transfers(self, timestamp_to_prune: int) -> None:
        """Prune transfers before timestamp_to_prune"""
        transfer_timestamps: list[int] = list()

        for token_addr, token_transfers in list(self._transfer_logs.items()):
            for _transfer_timestamp in list(token_transfers.keys()):
                if int(_transfer_timestamp) < timestamp_to_prune:
                    del token_transfers[_transfer_timestamp]
                else:
                    transfer_timestamps.append(int(_transfer_timestamp))
            if not token_transfers:
                del self._transfer_logs[token_addr]

        # update oldestTransferAt
        if transfer_timestamps:
            transfer_timestamps.sort()
            self.oldest_transfer_at = transfer_timestamps[0]
        else:
            self.oldest_transfer_at = None
