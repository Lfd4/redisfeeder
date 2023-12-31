# redisfeeder

A simple tool to push logs from stdin to a Redis instance.

## Usage
```
Read lines from stdin and send them to redis.

Usage: redisfeeder [OPTIONS] <HOSTNAME> <STORE_COMMAND> <KEY>

Arguments:
  <HOSTNAME>       redis host
  <STORE_COMMAND>  redis command to store the line [possible values: rpush]
  <KEY>            key to store the line into

Options:
  -t, --timeout <TIMEOUT>        timeout in seconds [default: 5]
  -d, --delay <DELAY>            time to wait before reconnecting in seconds [default: 5]
  -q, --queue-size <QUEUE_SIZE>  [default: 1024]
  -h, --help                     Print help
  -V, --version                  Print version
```

## License
Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
