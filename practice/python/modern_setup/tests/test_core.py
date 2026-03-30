from modern_setup.core import print_message


def test_print_message_basic() -> None:
    assert print_message("world") == "Hello, world"
