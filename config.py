import os

from dotenv import load_dotenv

_ = load_dotenv()


class ArangoDBConfig:
    HOST: str = os.environ.get("ARANGODB_HOST", "0.0.0.0")
    PORT: str = os.environ.get("ARANGODB_PORT", "8529")
    USERNAME: str = os.environ.get("ARANGODB_USERNAME", "root")
    PASSWORD: str = os.environ.get("ARANGODB_PASSWORD", "dev123")

    CONNECTION_URL: str = (
        os.getenv("ARANGODB_CONNECTION_URL")
        or f"arangodb@{USERNAME}:{PASSWORD}@http://{HOST}:{PORT}"
    )

    DATABASE: str = os.getenv("ARANGODB_DATABASE", "wallet_graph")
    GRAPH: str = "knowledge_graph"


class PostgresDBConfig:
    SCHEMA: str = os.environ.get("POSTGRES_SCHEMA", "public")
    TRANSFER_EVENT_TABLE: str = os.environ.get(
        "POSTGRES_TRANSFER_EVENT_TABLE", "transfer_event"
    )
    CONNECTION_URL: str = os.environ.get(
        "POSTGRES_CONNECTION_URL", "postgresql://user:password@localhost:5432/database"
    )


class BlockchainETLConfig:
    HOST: str = os.getenv("BLOCKCHAIN_ETL_HOST", "")
    PORT: str = os.getenv("BLOCKCHAIN_ETL_PORT", "")
    USERNAME: str = os.getenv("BLOCKCHAIN_ETL_USERNAME", "")
    PASSWORD: str = os.getenv("BLOCKCHAIN_ETL_PASSWORD", "")

    CONNECTION_URL: str = (
        os.getenv("BLOCKCHAIN_ETL_CONNECTION_URL")
        or f"mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}"
    )
    DATABASE: str = "blockchain_etl"
    DB_PREFIX: str = os.getenv("DB_PREFIX", "")


class MongoDBConfig:
    CONNECTION_URL: str = os.getenv(
        "MONGODB_CONNECTION_URL", "mongodb://131.153.202.197:28017"
    )
    DATABASE: str = os.getenv("MONGODB_DATABASE", "knowledge_graph")


class MongoDBEntityConfig:
    CONNECTION_URL: str = os.getenv(
        "MONGODB_ENTITY_CONNECTION_URL",
        "mongodb://klgReader:klgReaderEntity_910@178.128.85.210:27017,104.248.148.66:27017,103.253.146.224:27017/",
    )
    DATABASE: str = os.getenv("MONGODB_ENTITY_DATABASE", "knowledge_graph")


class MongoDBSmartContractConfig:
    CONNECTION_URL: str = os.getenv("MONGODB_SMARTCONTRACT_CONNECTION_URL", "")
    DATABASE: str = os.getenv("MONGODB_SMARTCONTRACT_DATABASE", "SmartContractLabel")


class Config:
    RUN_SETTING: dict[str, str | int | bool] = {
        "host": os.environ.get("SERVER_HOST", "localhost"),
        "port": int(os.environ.get("SERVER_PORT", 8080)),
        "debug": os.getenv("DEBUG", False),
        "access_log": False,
        "auto_reload": True,
        "workers": int(os.getenv("SERVER_WORKERS", 4)),
    }
    # uWSGI를 통해 배포되어야 하므로, production level에선 run setting을 건드리지 않음

    SECRET: str = os.environ.get("SECRET_KEY", "example project")
    JWT_PASSWORD: str = os.getenv("JWT_PASSWORD", "dev123")
    EXPIRATION_JWT: int = 2592000  # 1 month
    RESPONSE_TIMEOUT: int = 900  # seconds

    FALLBACK_ERROR_FORMAT: str = "json"

    OAS_UI_DEFAULT: str = "swagger"
    SWAGGER_UI_CONFIGURATION: dict[str, str] = {
        "apisSorter": "alpha",
        "docExpansion": "list",
        "operationsSorter": "alpha",
    }

    API_HOST: str = os.getenv("API_HOST", "0.0.0.0:8096")
    API_SCHEMES: str = os.getenv("API_SCHEMES", "http")
    API_VERSION: str = os.getenv("API_VERSION", "0.1.0")
    API_TITLE: str = os.getenv("API_TITLE", "Centic API")
    API_DESCRIPTION: str = os.getenv("API_DESCRIPTION", "Swagger for Centic API")
    API_CONTACT_EMAIL: str = os.getenv("API_CONTACT_EMAIL", "example@gmail.com")
