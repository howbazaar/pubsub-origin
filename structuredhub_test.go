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
	Origin  string `json:"origin"`
	Message string `json:"message"`
	ID      int    `json:"id"`
}

type JustOrigin struct {
	Origin string `json:"origin"`
}

type MessageID struct {
	Message string `json:"message"`
	Key     int    `json:"id"`
}

func (*StructuredHubSuite) TestPublishDeserialize(c *gc.C) {
	source := Emitter{
		Origin:  "test",
		Message: "hello world",
		ID:      42,
	}
	count := int32(0)
	hub := pubsub.NewStructuredHub(nil)
	_, err := hub.Subscribe("testing", func(topic string, data JustOrigin, err error) {
		c.Check(err, jc.ErrorIsNil)
		c.Check(topic, gc.Equals, "testing")
		c.Check(data.Origin, gc.Equals, source.Origin)
		atomic.AddInt32(&count, 1)
	})
	c.Assert(err, jc.ErrorIsNil)
	_, err = hub.Subscribe("testing", func(topic string, data MessageID, err error) {
		c.Check(err, jc.ErrorIsNil)
		c.Check(topic, gc.Equals, "testing")
		c.Check(data.Message, gc.Equals, source.Message)
		c.Check(data.Key, gc.Equals, source.ID)
		atomic.AddInt32(&count, 1)
	})
	c.Assert(err, jc.ErrorIsNil)
	_, err = hub.Subscribe("testing", func(topic string, data map[string]interface{}, err error) {
		c.Check(err, jc.ErrorIsNil)
		c.Check(topic, gc.Equals, "testing")
		c.Check(data, jc.DeepEquals, map[string]interface{}{
			"origin":  "test",
			"message": "hello world",
			"id":      float64(42), // ints are converted to floats through json.
		})
		atomic.AddInt32(&count, 1)
	})
	c.Assert(err, jc.ErrorIsNil)
	result, err := hub.Publish("testing", source)
	c.Assert(err, jc.ErrorIsNil)

	select {
	case <-result.Complete():
	case <-time.After(veryShortTime):
		c.Fatal("publish did not complete")
	}
	// Make sure they were all called.
	c.Assert(count, gc.Equals, int32(3))
}

func (*StructuredHubSuite) TestPublishMap(c *gc.C) {
	source := map[string]interface{}{
		"origin":  "test",
		"message": "hello world",
		"id":      42,
	}
	count := int32(0)
	hub := pubsub.NewStructuredHub(nil)
	_, err := hub.Subscribe("testing", func(topic string, data JustOrigin, err error) {
		c.Check(err, jc.ErrorIsNil)
		c.Check(topic, gc.Equals, "testing")
		c.Check(data.Origin, gc.Equals, source["origin"])
		atomic.AddInt32(&count, 1)
	})
	c.Assert(err, jc.ErrorIsNil)
	_, err = hub.Subscribe("testing", func(topic string, data MessageID, err error) {
		c.Check(err, jc.ErrorIsNil)
		c.Check(topic, gc.Equals, "testing")
		c.Check(data.Message, gc.Equals, source["message"])
		c.Check(data.Key, gc.Equals, source["id"])
		atomic.AddInt32(&count, 1)
	})
	c.Assert(err, jc.ErrorIsNil)
	_, err = hub.Subscribe("testing", func(topic string, data map[string]interface{}, err error) {
		c.Check(err, jc.ErrorIsNil)
		c.Check(topic, gc.Equals, "testing")
		c.Check(data, jc.DeepEquals, map[string]interface{}{
			"origin":  "test",
			"message": "hello world",
			"id":      42, // published maps don't go through the json map conversino
		})
		atomic.AddInt32(&count, 1)
	})
	c.Assert(err, jc.ErrorIsNil)
	result, err := hub.Publish("testing", source)
	c.Assert(err, jc.ErrorIsNil)

	select {
	case <-result.Complete():
	case <-time.After(veryShortTime):
		c.Fatal("publish did not complete")
	}
	// Make sure they were all called.
	c.Assert(count, gc.Equals, int32(3))
}
