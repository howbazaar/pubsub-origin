// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package pubsub_test

import (
	"sync/atomic"
	"time"

	"github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
	"gopkg.in/yaml.v2"

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

type BadID struct {
	ID string `json:"id"`
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

func (*StructuredHubSuite) TestPublishDeserializeError(c *gc.C) {
	source := Emitter{
		Origin:  "test",
		Message: "hello world",
		ID:      42,
	}
	count := int32(0)
	hub := pubsub.NewStructuredHub(nil)
	_, err := hub.Subscribe("testing", func(topic string, data BadID, err error) {
		c.Check(err.Error(), gc.Equals, "json unmarshalling: json: cannot unmarshal number into Go value of type string")
		c.Check(topic, gc.Equals, "testing")
		c.Check(data.ID, gc.Equals, "")
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
	c.Assert(count, gc.Equals, int32(1))
}

type yamlMarshaller struct{}

func (*yamlMarshaller) Marshal(v interface{}) ([]byte, error) {
	return yaml.Marshal(v)
}

func (*yamlMarshaller) Unmarshal(data []byte, v interface{}) error {
	return yaml.Unmarshal(data, v)
}

func (*StructuredHubSuite) TestYAMLMarshalling(c *gc.C) {
	source := Emitter{
		Origin:  "test",
		Message: "hello world",
		ID:      42,
	}
	count := int32(0)
	hub := pubsub.NewStructuredHub(
		&pubsub.StructuredHubConfig{
			Marshaller: &yamlMarshaller{},
		})
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
		// Key is zero because there is no yaml serialization directive, and Key != ID.
		c.Check(data.Key, gc.Equals, 0)
		atomic.AddInt32(&count, 1)
	})
	c.Assert(err, jc.ErrorIsNil)
	_, err = hub.Subscribe("testing", func(topic string, data map[string]interface{}, err error) {
		c.Check(err, jc.ErrorIsNil)
		c.Check(topic, gc.Equals, "testing")
		c.Check(data, jc.DeepEquals, map[string]interface{}{
			"origin":  "test",
			"message": "hello world",
			"id":      42, // yaml serializes integers just fine.
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

func (*StructuredHubSuite) TesAnnotations(c *gc.C) {
	source := Emitter{
		Message: "hello world",
		ID:      42,
	}
	origin := "master"
	obtained := []string{}
	hub := pubsub.NewStructuredHub(
		&pubsub.StructuredHubConfig{
			Annotations: map[string]interface{}{
				"origin": origin,
			},
		})
	_, err := hub.Subscribe("testing", func(topic string, data Emitter, err error) {
		c.Check(err, jc.ErrorIsNil)
		c.Check(topic, gc.Equals, "testing")
		obtained = append(obtained, data.Origin)
		c.Check(data.Message, gc.Equals, source.Message)
		c.Check(data.ID, gc.Equals, source.ID)
	})
	c.Assert(err, jc.ErrorIsNil)
	result, err := hub.Publish("testing", source)
	c.Assert(err, jc.ErrorIsNil)

	select {
	case <-result.Complete():
	case <-time.After(veryShortTime):
		c.Fatal("publish did not complete")
	}

	source.Origin = "other"
	result, err = hub.Publish("testing", source)
	c.Assert(err, jc.ErrorIsNil)

	select {
	case <-result.Complete():
	case <-time.After(veryShortTime):
		c.Fatal("publish did not complete")
	}
	c.Assert(obtained, jc.DeepEquals, []string{origin, "other"})
}
