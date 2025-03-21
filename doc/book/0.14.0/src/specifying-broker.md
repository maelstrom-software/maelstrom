# Specifying the Broker

Every client has a `broker` configuration value that specifies the socket
address of the broker. This configuration value is optional. If not provided,
the client will run in [standalone mode](local-worker.md).

Here are some example socket address values:
  - `broker.example.org:1234`
  - `192.0.2.3:1234`
  - `[2001:db8::3]:1234`
