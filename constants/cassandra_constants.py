class CassandraConstants:
    """
    Cassandra constants for the address clustering ETL job.
    """

    # Keyspace and table names
    schema: str = "blockchain_etl"

    block_table: str = "blocks"
    transfer_event_table: str = "token_transfer"
    transactions_table: str = "transactions"
    bucket_size_transfer_event: int = 100
    bucket_size_block: int = 10000
    bucket_size_transactions: int = 100
