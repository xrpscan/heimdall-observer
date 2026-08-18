# Heimdall Observer

Connects to an [xrpld](https://xrpl.org/) node via WebSocket, subscribes to the validation and ledger streams, batches incoming messages into an embedded SQLite database, and produces them to Kafka.

## How it works

1. **Stream Processors** subscribe to a xrpld node's `validations` and `ledger` streams. Incoming messages are batched and inserted into their respective SQLite tables.

2. **Kafka Synchronizers** poll each SQLite table on an interval, produce each batch to its Kafka topic, and delete the rows after successful production. SQLite acts as a durable buffer between the WebSocket streams and Kafka.

## Quick start

```bash
# 1. Copy and edit the config.
cp config/config.example.json config/config.json

# 2. Run database migrations.
migrate -verbose -path db/migrations -database sqlite3://path/to/db up

# 3. Build application binary.
go build -o bin/observer cmd/observer/main.go

# 4. Run.
./bin/observer -config config/config.json
```

The binary accepts `-config <path>` to specify a custom config file (default: `config/config.json`).

## Configuration

All fields are required unless noted otherwise.

| Section | Field | Description |
|---|---|---|
| `database` | `filePath` | Path to the SQLite database file. Parent directories are created automatically. |
| `httpServer` | `addr` | Listen address for the HTTP server (e.g. `localhost:8080`). |
| | `allowedOrigins` | CORS allowed origins (e.g. `["*"]`). |
| | `corsMaxAgeSec` | CORS preflight cache duration in seconds. |
| `kafka` | `brokers` | List of Kafka broker addresses. |
| | `username` | _(optional)_ username for SCRAM-SHA. If not provided, SCRAM-SHA and TLS are disabled. |
| | `password` | _(optional)_ password for SCRAM-SHA. If not provided, SCRAM-SHA and TLS are disabled. |
| | `caCertPath` | _(optional)_ Path to a custom CA certificate for TLS. If omitted, system CAs are used. |
| | `validationsTopic` | Kafka topic to produce validation messages to. |
| | `ledgerTopic` | Kafka topic to produce ledger messages to. |
| `logger` | `filePath` | _(optional)_ Log file path. If empty, logs to stdout. Log rotation is handled automatically. |
| | `level` | Log level: `debug`, `info`, `warn`, or `error`. |
| | `pretty` | `true` for key=value format, `false` for JSON. |
| `xrpl` | `addr` | WebSocket URL of the xrpld node (e.g. `wss://xrplcluster.com`). |
| `validationStreamProcessor` | `maxBatchSize` | Number of validation messages to accumulate before flushing to SQLite. |
| | `autoFlushDelaySec` | Seconds to wait before auto-flushing a partial batch to SQLite. |
| `ledgerStreamProcessor` | `maxBatchSize` | Number of ledger messages to accumulate before flushing to SQLite. |
| | `autoFlushDelaySec` | Seconds to wait before auto-flushing a partial batch to SQLite. |
| `validationKafkaSynchronizer` | `maxBatchSize` | Max validation messages to produce into Kafka in a single payload. |
| | `pollIntervalSec` | Seconds between each poll of the validations table. |
| `ledgerKafkaSynchronizer` | `maxBatchSize` | Max ledger messages to produce into Kafka in a single payload. |
| | `pollIntervalSec` | Seconds between each poll of the ledger table. |

See [`config/config.example.json`](config/config.example.json) for a complete example.

## Build & test

```bash
make build       # Compile to bin/observer
make test        # Run tests with race detector
make lint        # Run golangci-lint
make image       # Build container image
make container   # Run container locally
```
