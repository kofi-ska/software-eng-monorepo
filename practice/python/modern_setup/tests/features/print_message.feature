Feature: Print a formatted message
  Scenario: Basic greeting
    Given a message "world"
    When I call print_message
    Then the result should be "Hello, world"
