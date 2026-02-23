from utils.logger_utils import get_logger

logger = get_logger("Parser")


def get_connection_elements(string: str) -> tuple[str, str, str]:
    """
    example output for exporter_type: exporter_type@username:password@connection_url

    :param string:
    :return: username, password, connection_url
    """
    try:
        elements = string.split("@")
        auth = elements[1].split(":")
        username = auth[0]
        password = auth[1]
        connection_url = elements[2]
        return username, password, connection_url
    except Exception as e:
        logger.warning(f"get_connection_elements err {e}")
        return None, None, None
