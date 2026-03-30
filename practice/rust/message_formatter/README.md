# Message Formatter

## Overview

This project implements a library function which formats a message based on the
specified format

# Design

- A private helper `build_message` is used to implement the formatting approach
- A public function `format_message` is then used to call the helper with the message
  as input to apply the formatting rule

## Tests

These include

- unit tests to handle edge cases
- integration tests to assert UX

In project folder run

```bash
cargo check
cargo build
cargo test
```
