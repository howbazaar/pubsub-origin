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

type SimpleHubSuite struct {
	testing.LoggingCleanupSuite
}

var (
	_ = gc.Suite(&SimpleHubSuite{})

	veryShortTime = time.Millisecond
)

func (*SimpleHubSuite) TestPublishNoSubscribers(c *gc.C) {
	hub := pubsub.NewSimpleHub()
	result := hub.Publish("testing", nil)

	select {
	case <-result.Complete():
	case <-time.After(veryShortTime):
		c.Fatal("publish did not complete")
	}
}

func (*SimpleHubSuite) TestPublishOneSubscriber(c *gc.C) {
	var called bool
	hub := pubsub.NewSimpleHub()
	hub.Subscribe("testing", func(topic string, data interface{}) {
		c.Check(topic, gc.Equals, "testing")
		c.Check(data, gc.IsNil)
		called = true
	})
	result := hub.Publish("testing", nil)

	select {
	case <-result.Complete():
	case <-time.After(veryShortTime):
		c.Fatal("publish did not complete")
	}
	c.Assert(called, jc.IsTrue)
}

func (*SimpleHubSuite) TestPublishCompleterWaits(c *gc.C) {
	wait := make(chan struct{})
	hub := pubsub.NewSimpleHub()
	hub.Subscribe("testing", func(topic string, data interface{}) {
		<-wait
	})
	result := hub.Publish("testing", nil)

	select {
	case <-result.Complete():
		c.Fatal("didn't wait")
	case <-time.After(veryShortTime):
	}

	close(wait)

	select {
	case <-result.Complete():
	case <-time.After(veryShortTime):
		c.Fatal("publish did not complete")
	}
}

func (*SimpleHubSuite) TestSubscribeRegexError(c *gc.C) {
	hub := pubsub.NewSimpleHub()
	_, err := hub.Subscribe("*", func(topic string, data interface{}) {})
	c.Assert(err, gc.ErrorMatches, "topic should be a regex string: error parsing regexp: .*")
}

func (*SimpleHubSuite) TestSubscribeMissingHandler(c *gc.C) {
	hub := pubsub.NewSimpleHub()
	_, err := hub.Subscribe("test", nil)
	c.Assert(err, gc.ErrorMatches, "missing handler not valid")
}

func (*SimpleHubSuite) TestSubscriberRegex(c *gc.C) {
	count := int32(0)
	callback := func(string, interface{}) {
		atomic.AddInt32(&count, 1)
	}
	hub := pubsub.NewSimpleHub()
	hub.Subscribe("testing", callback)
	hub.Subscribe("other", callback)
	hub.Subscribe("test.*", callback)
	hub.Subscribe("tes.ing", callback)

	result := hub.Publish("testing", nil)

	select {
	case <-result.Complete():
	case <-time.After(veryShortTime):
		c.Fatal("publish did not complete")
	}
	c.Assert(count, gc.Equals, int32(3))
}

func (*SimpleHubSuite) TestUnsubscribe(c *gc.C) {
	var called bool
	hub := pubsub.NewSimpleHub()
	result, err := hub.Subscribe("testing", func(topic string, data interface{}) {
		called = true
	})
	c.Assert(err, jc.ErrorIsNil)
	result.Unsubscribe()
	result := hub.Publish("testing", nil)

	select {
	case <-result.Complete():
	case <-time.After(veryShortTime):
		c.Fatal("publish did not complete")
	}
	c.Assert(called, jc.IsFalse)
}
