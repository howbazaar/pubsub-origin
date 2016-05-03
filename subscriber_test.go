// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package pubsub_test

import (
	"github.com/juju/testing"
	gc "gopkg.in/check.v1"
)

type SubscriberSuite struct {
	testing.LoggingCleanupSuite
}

var _ = gc.Suite(&SubscriberSuite{})
