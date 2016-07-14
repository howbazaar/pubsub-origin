// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package pubsub_test

import (
	"time"

	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"

	"github.com/juju/pubsub"
)

const numSubscribers = 100

var _ = gc.Suite(&BenchmarkSuite{})

type BenchmarkSuite struct{}

func (*BenchmarkSuite) BenchmarkSimplePublishAndWaitNoSubscribers(c *gc.C) {
	hub := pubsub.NewSimpleHub()
	topic := pubsub.Topic("benchmarking")
	failedCount := 0
	for i := 0; i < c.N; i++ {
		result, err := hub.Publish(topic, nil)
		c.Assert(err, jc.ErrorIsNil)

		select {
		case <-result.Complete():
		case <-time.After(5 * veryShortTime):
			failedCount++
		}
	}
	// XXX: on my VM, this fails about half the time.
	c.Assert(failedCount, gc.Equals, 0)
}

func (*BenchmarkSuite) BenchmarkSimplePublishAndWaitOneSubscriber(c *gc.C) {
	hub := pubsub.NewSimpleHub()
	topic := pubsub.Topic("benchmarking")
	counter := 0
	sub, err := hub.Subscribe(topic, func(topic pubsub.Topic, data interface{}) {
		counter += 1
	})
	defer sub.Unsubscribe()
	c.Assert(err, jc.ErrorIsNil)
	failedCount := 0
	for i := 0; i < c.N; i++ {
		result, err := hub.Publish(topic, nil)
		c.Assert(err, jc.ErrorIsNil)

		select {
		case <-result.Complete():
		case <-time.After(5 * veryShortTime):
			failedCount++
		}
	}
	// XXX: on my VM, this fails about half the time.
	c.Check(failedCount, gc.Equals, 0)
	c.Check(counter, gc.Equals, c.N)
}

func (*BenchmarkSuite) BenchmarkSimplePublishAndWaitManySubscribers(c *gc.C) {
	hub := pubsub.NewSimpleHub()
	topic := pubsub.Topic("benchmarking")
	counters := make([]int, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		i := i
		sub, err := hub.Subscribe(topic, func(topic pubsub.Topic, data interface{}) {
			counters[i]++
		})
		c.Assert(err, jc.ErrorIsNil)
		defer sub.Unsubscribe()
	}
	failedCount := 0
	for i := 0; i < c.N; i++ {
		result, err := hub.Publish(topic, nil)
		c.Assert(err, jc.ErrorIsNil)

		select {
		case <-result.Complete():
		case <-time.After(5 * veryShortTime):
			failedCount++
		}
	}
	c.Check(failedCount, gc.Equals, 0)
	total := 0
	expected := make([]int, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		expected[i] = c.N
		total += counters[i]
	}
	c.Check(total, gc.Equals, c.N*numSubscribers)
}

func (*BenchmarkSuite) BenchmarkStructuredPublishAndWaitOneSubscriber(c *gc.C) {
	source := Emitter{
		Origin:  "test",
		Message: "hello world",
		ID:      42,
	}
	hub := pubsub.NewStructuredHub(nil)
	topic := pubsub.Topic("benchmarking")
	counter := 0
	sub, err := hub.Subscribe(topic, func(topic pubsub.Topic, s Emitter, err error) {
		counter++
	})
	c.Assert(err, jc.ErrorIsNil)
	defer sub.Unsubscribe()
	failedCount := 0
	for i := 0; i < c.N; i++ {
		// Publishing nil results in a nil-pointer dereference
		result, err := hub.Publish(topic, source)
		c.Assert(err, jc.ErrorIsNil)

		select {
		case <-result.Complete():
		case <-time.After(5 * veryShortTime):
			failedCount++
		}
	}
	c.Check(failedCount, gc.Equals, 0)
	c.Check(counter, gc.Equals, c.N)
}

func (*BenchmarkSuite) BenchmarkStructuredPublishAndWaitManySubscribers(c *gc.C) {
	source := Emitter{
		Origin:  "test",
		Message: "hello world",
		ID:      42,
	}
	hub := pubsub.NewStructuredHub(nil)
	topic := pubsub.Topic("benchmarking")
	counters := make([]int, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		mycounter := i
		sub, err := hub.Subscribe(topic, func(topic pubsub.Topic, s Emitter, err error) {
			counters[mycounter]++
		})
		c.Assert(err, jc.ErrorIsNil)
		defer sub.Unsubscribe()
	}
	failedCount := 0
	for i := 0; i < c.N; i++ {
		// Publishing nil results in a nil-pointer dereference
		result, err := hub.Publish(topic, source)
		c.Assert(err, jc.ErrorIsNil)

		select {
		case <-result.Complete():
		case <-time.After(5 * veryShortTime):
			failedCount++
		}
	}
	// XXX: on my machine, this fails about half the time if I use the same 1ms
	c.Check(failedCount, gc.Equals, 0)
	total := 0
	expected := make([]int, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		expected[i] = c.N
		total += counters[i]
	}
	c.Check(counters, gc.DeepEquals, expected)
	c.Check(total, gc.Equals, c.N*numSubscribers)
}

func (*BenchmarkSuite) BenchmarkPublishAndWaitMultiplexedOneSubscriber(c *gc.C) {
	source := Emitter{
		Origin:  "test",
		Message: "hello world",
		ID:      42,
	}
	hub := pubsub.NewStructuredHub(nil)
	sub, multi, err := pubsub.NewMultiplexer(hub)
	c.Assert(err, jc.ErrorIsNil)
	defer sub.Unsubscribe()
	topic := pubsub.Topic("benchmarking")
	counter := 0
	err = multi.Add(pubsub.MatchAll, func(topic pubsub.Topic, s Emitter, err error) {
		counter++
	})
	c.Assert(err, jc.ErrorIsNil)
	failedCount := 0
	for i := 0; i < c.N; i++ {
		// Publishing nil results in a nil-pointer dereference
		result, err := hub.Publish(topic, source)
		c.Assert(err, jc.ErrorIsNil)

		select {
		case <-result.Complete():
		case <-time.After(5 * veryShortTime):
			failedCount++
		}
	}
	// XXX: on my machine, this fails about half the time if I use the same 1ms
	c.Check(failedCount, gc.Equals, 0)
	c.Check(counter, gc.Equals, c.N)
}

func (*BenchmarkSuite) BenchmarkPublishAndWaitMultiplexedSubscribers(c *gc.C) {
	source := Emitter{
		Origin:  "test",
		Message: "hello world",
		ID:      42,
	}
	hub := pubsub.NewStructuredHub(nil)
	sub, multi, err := pubsub.NewMultiplexer(hub)
	c.Assert(err, jc.ErrorIsNil)
	defer sub.Unsubscribe()
	topic := pubsub.Topic("benchmarking")
	counters := make([]int, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		mycounter := i
		err := multi.Add(pubsub.MatchAll, func(topic pubsub.Topic, s Emitter, err error) {
			counters[mycounter]++
		})
		c.Assert(err, jc.ErrorIsNil)
	}
	failedCount := 0
	for i := 0; i < c.N; i++ {
		// Publishing nil results in a nil-pointer dereference
		result, err := hub.Publish(topic, source)
		c.Assert(err, jc.ErrorIsNil)

		select {
		case <-result.Complete():
		case <-time.After(5 * veryShortTime):
			failedCount++
		}
	}
	// XXX: on my machine, this fails about half the time if I use the same 1ms
	c.Check(failedCount, gc.Equals, 0)
	total := 0
	for i := 0; i < numSubscribers; i++ {
		c.Check(counters[i], gc.Equals, c.N,
			gc.Commentf("wrong counter amount %d != %d for %d", counters[i], c.N, i))
		total += counters[i]
	}
	c.Check(total, gc.Equals, c.N*numSubscribers)
}
