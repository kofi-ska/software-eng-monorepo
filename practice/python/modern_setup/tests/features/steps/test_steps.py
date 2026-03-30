from modern_setup.core import print_message
from pytest_bdd import given, scenarios, then, when

scenarios("../print_message.feature")


@given('a message "world"')
def message() -> str:
    return "world"


@when("I call print_message")
def call_print(message: str) -> str:
    return print_message(message)


@then('the result should be "Hello, world"')
def check_result(call_print: str) -> None:
    assert call_print == "Hello, world"
