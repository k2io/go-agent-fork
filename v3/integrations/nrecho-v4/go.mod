module github.com/newrelic/go-agent/v3/integrations/nrecho-v4

// As of Jun 2022, the echo go.mod file uses 1.17:
// https://github.com/labstack/echo/blob/master/go.mod
go 1.20

require (
	github.com/labstack/echo/v4 v4.9.0
	github.com/newrelic/go-agent/v3 v3.32.0
)


replace github.com/newrelic/go-agent/v3 => ../..
