// Copyright 2020 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package nrsecurityagent

import (
	"testing"

	"github.com/newrelic/go-agent/v3/internal/integrationsupport"
	newrelic "github.com/newrelic/go-agent/v3/newrelic"
)

func TestSecurityAgentInitilization(t *testing.T) {
	app := integrationsupport.NewTestApp(nil)
	err := InitSecurityAgent(
		app.Application,
		ConfigSecurityEnable(true),
	)
	if err != nil {
		t.Error("Security agent is not initialized. Error: ", err)
	} else {
		if err, ok := newrelic.GetSecurityAgentInterface().SendEvent("UNIT_TEST").(error); ok && err != nil {
			t.Error("Error occurred while sending event with the Security Agent API. Details: ", err.Error())
		}
	}
	return
}
