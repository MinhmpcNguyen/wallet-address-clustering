from databases.arangodb_klg import AddressGraphClient


class AddrGraphClient:
    _arangodb_service: AddressGraphClient | None = None

    @classmethod
    def get_arangodb_service(cls, chain_id: str) -> AddressGraphClient:
        """
        Get an instance of HttpService.
        """
        if not cls._arangodb_service:
            cls._arangodb_service = AddressGraphClient(chain_id)
        return cls._arangodb_service
