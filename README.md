# Two Phase Kafka Pattern

Two phase commit pattern for kafka in golang

## Setup

`docker-compose up -d`

## Topics

- control.setup
- control.commit
- control.cancel
- data.test
- event.test

## Endpoints

- localhost:8080/setup
- localhost:8080/cancel
- localhost:8080/commit/test
- localhost:8080/event/test

## Headers

- ContextID: mandatory for all endpoints, guid for logging
- CallbackPath: mandatory for setup endpoint
- TransactionID: mandatory for commit and cancel endpoints
- Key: mandatory for commit and event endpoints

## Examples

`curl -X POST -H 'ContextID: abc' -H 'CallbackPath: localhost:8081/callback&id=5' -v localhost:8080/setup`

`curl -X POST -H 'ContextID: abc' -H 'TransactionID: test' -v localhost:8080/cancel`

`curl -X POST -H 'ContextID: abc' -H 'TransactionID: test' -H 'Key: testKey' -v localhost:8080/commit/test -d '{"example":true}'`

`curl -X POST -H 'ContextID: abc' -H 'Key: testKey' -v localhost:8080/event/test -d '{"example":true}'`
