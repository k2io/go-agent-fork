module github.com/newrelic/go-agent/v3/integrations/nrstan/test

// This module exists to avoid a dependency on
// github.com/nats-io/nats-streaming-server in nrstan.
go 1.19

require (
	github.com/nats-io/nats-streaming-server v0.25.5
	github.com/nats-io/stan.go v0.10.4
	github.com/newrelic/go-agent/v3 v3.26.0
	github.com/newrelic/go-agent/v3/integrations/nrstan v0.0.0
)


replace github.com/newrelic/go-agent/v3/integrations/nrstan => ../

replace github.com/newrelic/go-agent/v3 => ../../..
