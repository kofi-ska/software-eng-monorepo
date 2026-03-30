
EXCHANGE_REGISTRY = {}


def register_exchange(name: str):
    """
    Decorator for registering new exchange plugins dynamically.
    Example:
        @register_exchange("binance")
        class BinanceFeed(ExchangeFeed): ...
    """

    def decorator(cls):
        EXCHANGE_REGISTRY[name.lower()] = cls
        return cls

    return decorator


def get_exchange_class(name: str):
    """Retrieve a registered exchange class by name."""
    return EXCHANGE_REGISTRY.get(name.lower())


def list_registered_exchanges():
    """Return list of all available exchange names."""
    return list(EXCHANGE_REGISTRY.keys())
