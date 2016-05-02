// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package pubsub_test

import (
	"sync/atomic"
	"time"

	"github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"

	"github.com/juju/pubsub"
)

type StructuredHubSuite struct {
	testing.LoggingCleanupSuite
}

var _ = gc.Suite(&StructuredHubSuite{})

type Emitter struct {
	Origin  string `yaml:"origin"`
	Message string `yaml:"message"`
	ID      int    `yaml:"id"`
}

type JustOrigin struct {
	Origin string `yaml:"origin"`
}

type MessageID struct {
	Message string `yaml:"message"`
	ID      int    `yaml:"id"`
}

func (*StructuredHubSuite) TestPublishDeserialize(c *gc.C) {
	source := Emitter{
		Origin:  "test",
		Message: "hello world",
		ID:      42,
	}
	count := int32(0)
	hub := pubsub.NewStructuredHub()
	hub.Subscribe("testing", func(topic string, data JustOrigin) {
		c.Check(topic, gc.Equals, "testing")
		c.Check(data.Origin, gc.Equals, source.Origin)
		atomic.AddInt32(&count, 1)
	})
	hub.Subscribe("testing", func(topic string, data MessageID) {
		c.Check(topic, gc.Equals, "testing")
		c.Check(data.Message, gc.Equals, source.Message)
		c.Check(data.ID, gc.Equals, source.ID)
		atomic.AddInt32(&count, 1)
	})
	result, err := hub.Publish("testing", source)
	c.Assert(err, jc.ErrorIsNil)

	select {
	case <-result.Complete():
	case <-time.After(veryShortTime):
		c.Fatal("publish did not complete")
	}
	// Make sure they were both called.
	c.Assert(count, gc.Equals, int32(2))
}
