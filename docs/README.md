# Documentation

[Project README](../README.md)

These guides describe the current Beehive InfluxDB loader source. They do not mean that a deployed loader has archival coordination enabled.

## Choose a starting point

| Audience | Reading path |
| --- | --- |
| Operators | [Operations](operations.md), then [configuration](configuration.md) |
| Maintainers | [Architecture](architecture.md), then [development](development.md) |

## Guides

- [Architecture](architecture.md): batching, validation, hot and late routing, acknowledgement, quarantine, and backup admission pause.
- [Configuration](configuration.md): startup flags, environment variables, defaults, and fixed connection settings.
- [Operations](operations.md): local startup, dry-run canary, metrics, the quarantine queue, joining the archiver, and temporary backup maintenance.
- [Development](development.md): module layout, offline tests (including backup pause cases), and the local RabbitMQ and Influx stack.
