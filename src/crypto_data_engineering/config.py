import os
from dotenv import load_dotenv


load_dotenv()


def get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


COIN_LIMIT = get_int("COIN_LIMIT", 50)
VS_CURRENCY = os.getenv("VS_CURRENCY", "usd")
