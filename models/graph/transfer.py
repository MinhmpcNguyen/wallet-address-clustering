class Transfer:
    def __init__(
        self,
        chain_id: str,
        from_addr: str,
        to_addr: str,
        coin_addr: str,
        amount: float,
        timestamp: int,
    ):
        self.chain_id: str = chain_id
        self.from_addr: str = from_addr
        self.to_addr: str = to_addr
        self.coin_addr: str = coin_addr
        self.amount: float = amount
        self.timestamp: int = timestamp
