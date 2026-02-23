from databases.clickhouse import ClickHouseCentic


class ClickHouseClient:
    _clickhouse_service: ClickHouseCentic | None = None

    @classmethod
    def get_clickhouse_service(cls) -> ClickHouseCentic:
        """
        Get an instance of HttpService.
        """
        if not cls._clickhouse_service:
            cls._clickhouse_service = ClickHouseCentic()
        return cls._clickhouse_service
