// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package pubsub_test

import (
	"time"

	gc "gopkg.in/check.v1"
	jc "github.com/juju/testing/checkers"

	"github.com/juju/pubsub"
)

var _ = gc.Suite(&BenchmarkSuite{})

type BenchmarkSuite struct { }

func (*BenchmarkSuite) BenchmarkPublishAndWaitNoSubscribers(c *gc.C) {
	hub := pubsub.NewSimpleHub()
	topic := pubsub.Topic("benchmarking")
	failedCount := 0
	for i := 0; i < c.N; i++ {
		result, err := hub.Publish(topic, nil)
		c.Assert(err, jc.ErrorIsNil)

		select {
		case <-result.Complete():
		case <-time.After(veryShortTime):
			failedCount++
		}
	}
	// XXX: on my VM, this fails about half the time.
	c.Assert(failedCount, gc.Equals, 0)
}

func (*BenchmarkSuite) BenchmarkPublishAndWaitOneSubscriber(c *gc.C) {
	hub := pubsub.NewSimpleHub()
	topic := pubsub.Topic("benchmarking")
	counter := 0
	hub.Subscribe(topic, func(topic pubsub.Topic, data interface{}) {
		counter += 1
	})
	failedCount := 0
	for i := 0; i < c.N; i++ {
		result, err := hub.Publish(topic, nil)
		c.Assert(err, jc.ErrorIsNil)

		select {
		case <-result.Complete():
		case <-time.After(veryShortTime):
			failedCount++
		}
	}
	// XXX: on my VM, this fails about half the time.
	c.Check(failedCount, gc.Equals, 0)
	c.Check(counter, gc.Equals, c.N)
}
