// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package pubsub_test

import (
	"sync"
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
	var (
		originCalled  bool
		messageCalled bool
		mapCalled     bool
	)
	hub := pubsub.NewStructuredHub(nil)
	_, err := hub.Subscribe(topic, func(topic pubsub.Topic, data JustOrigin, err error) {
		c.Check(err, jc.ErrorIsNil)
		c.Check(topic, gc.Equals, topic)
		c.Check(data.Origin, gc.Equals, source.Origin)
		originCalled = true
	})
	c.Assert(err, jc.ErrorIsNil)
	_, err = hub.Subscribe(topic, func(topic pubsub.Topic, data MessageID, err error) {
		c.Check(err, jc.ErrorIsNil)
		c.Check(topic, gc.Equals, topic)
		c.Check(data.Message, gc.Equals, source.Message)
		c.Check(data.Key, gc.Equals, source.ID)
		messageCalled = true
	})
	c.Assert(err, jc.ErrorIsNil)
	_, err = hub.Subscribe(topic, func(topic pubsub.Topic, data map[string]interface{}, err error) {
		c.Check(err, jc.ErrorIsNil)
		c.Check(topic, gc.Equals, topic)
		c.Check(data, jc.DeepEquals, map[string]interface{}{
			"origin":  "test",
			"message": "hello world",
			"id":      float64(42), // ints are converted to floats through json.
		})
		mapCalled = true
	})
	c.Assert(err, jc.ErrorIsNil)
	result, err := hub.Publish(topic, source)
	c.Assert(err, jc.ErrorIsNil)

	select {
	case <-result.Complete():
	case <-time.After(veryShortTime):
		c.Fatal("publish did not complete")
	}
	// Make sure they were all called.
	c.Check(originCalled, jc.IsTrue)
	c.Check(messageCalled, jc.IsTrue)
	c.Check(mapCalled, jc.IsTrue)
}

func (*StructuredHubSuite) TestPublishMap(c *gc.C) {
	source := map[string]interface{}{
		"origin":  "test",
		"message": "hello world",
		"id":      42,
	}
	var (
		originCalled  bool
		messageCalled bool
		mapCalled     bool
	)
	hub := pubsub.NewStructuredHub(nil)
	_, err := hub.Subscribe(topic, func(topic pubsub.Topic, data JustOrigin, err error) {
		c.Check(err, jc.ErrorIsNil)
		c.Check(topic, gc.Equals, topic)
		c.Check(data.Origin, gc.Equals, source["origin"])
		originCalled = true
	})
	c.Assert(err, jc.ErrorIsNil)
	_, err = hub.Subscribe(topic, func(topic pubsub.Topic, data MessageID, err error) {
		c.Check(err, jc.ErrorIsNil)
		c.Check(topic, gc.Equals, topic)
		c.Check(data.Message, gc.Equals, source["message"])
		c.Check(data.Key, gc.Equals, source["id"])
		messageCalled = true
	})
	c.Assert(err, jc.ErrorIsNil)
	_, err = hub.Subscribe(topic, func(topic pubsub.Topic, data map[string]interface{}, err error) {
		c.Check(err, jc.ErrorIsNil)
		c.Check(topic, gc.Equals, topic)
		c.Check(data, jc.DeepEquals, map[string]interface{}{
			"origin":  "test",
			"message": "hello world",
			"id":      42, // published maps don't go through the json map conversino
		})
		mapCalled = true
	})
	c.Assert(err, jc.ErrorIsNil)
	result, err := hub.Publish(topic, source)
	c.Assert(err, jc.ErrorIsNil)

	select {
	case <-result.Complete():
	case <-time.After(veryShortTime):
		c.Fatal("publish did not complete")
	}
	// Make sure they were all called.
	c.Check(originCalled, jc.IsTrue)
	c.Check(messageCalled, jc.IsTrue)
	c.Check(mapCalled, jc.IsTrue)
}

func (*StructuredHubSuite) TestPublishDeserializeError(c *gc.C) {
	source := Emitter{
		Origin:  "test",
		Message: "hello world",
		ID:      42,
	}
	called := false
	hub := pubsub.NewStructuredHub(nil)
	_, err := hub.Subscribe(topic, func(topic pubsub.Topic, data BadID, err error) {
		c.Check(err.Error(), gc.Equals, "unmarshalling data: json: cannot unmarshal number into Go value of type string")
		c.Check(topic, gc.Equals, topic)
		c.Check(data.ID, gc.Equals, "")
		called = true
	})
	c.Assert(err, jc.ErrorIsNil)
	result, err := hub.Publish(topic, source)
	c.Assert(err, jc.ErrorIsNil)

	select {
	case <-result.Complete():
	case <-time.After(veryShortTime):
		c.Fatal("publish did not complete")
	}
	c.Assert(called, jc.IsTrue)
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
	called := false
	hub := pubsub.NewStructuredHub(
		&pubsub.StructuredHubConfig{
			Marshaller: &yamlMarshaller{},
		})
	_, err := hub.Subscribe(topic, func(topic pubsub.Topic, data map[string]interface{}, err error) {
		c.Check(err, jc.ErrorIsNil)
		c.Check(topic, gc.Equals, topic)
		c.Check(data, jc.DeepEquals, map[string]interface{}{
			"origin":  "test",
			"message": "hello world",
			"id":      42, // yaml serializes integers just fine.
		})
		called = true
	})
	c.Assert(err, jc.ErrorIsNil)
	result, err := hub.Publish(topic, source)
	c.Assert(err, jc.ErrorIsNil)

	select {
	case <-result.Complete():
	case <-time.After(veryShortTime):
		c.Fatal("publish did not complete")
	}
	// Make sure they were all called.
	c.Assert(called, jc.IsTrue)
}

func (*StructuredHubSuite) TestAnnotations(c *gc.C) {
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
	_, err := hub.Subscribe(topic, func(topic pubsub.Topic, data Emitter, err error) {
		c.Check(err, jc.ErrorIsNil)
		c.Check(topic, gc.Equals, topic)
		obtained = append(obtained, data.Origin)
		c.Check(data.Message, gc.Equals, source.Message)
		c.Check(data.ID, gc.Equals, source.ID)
	})
	c.Assert(err, jc.ErrorIsNil)
	result, err := hub.Publish(topic, source)
	c.Assert(err, jc.ErrorIsNil)

	select {
	case <-result.Complete():
	case <-time.After(veryShortTime):
		c.Fatal("publish did not complete")
	}

	source.Origin = "other"
	result, err = hub.Publish(topic, source)
	c.Assert(err, jc.ErrorIsNil)

	select {
	case <-result.Complete():
	case <-time.After(veryShortTime):
		c.Fatal("publish did not complete")
	}
	c.Assert(obtained, jc.DeepEquals, []string{origin, "other"})
}

type Worker struct {
	m          sync.Mutex
	fromStruct []string
	fromMap    []string
}

func (w *Worker) subMessage(topic pubsub.Topic, data MessageID, err error) {
	w.m.Lock()
	defer w.m.Unlock()

	w.fromStruct = append(w.fromStruct, data.Message)
}

func (w *Worker) subData(topic pubsub.Topic, data map[string]interface{}, err error) {
	w.m.Lock()
	defer w.m.Unlock()

	value, _ := data["message"].(string)
	w.fromMap = append(w.fromMap, value)
}

func (*StructuredHubSuite) TestMultipleSubscribersSingleInstance(c *gc.C) {
	hub := pubsub.NewStructuredHub(nil)
	w := &Worker{}
	_, err := hub.Subscribe(pubsub.MatchAll, w.subData)
	c.Assert(err, jc.ErrorIsNil)
	_, err = hub.Subscribe(pubsub.MatchAll, w.subMessage)
	c.Assert(err, jc.ErrorIsNil)

	message := "a message"
	result, err := hub.Publish("foo", MessageID{Message: message})
	c.Assert(err, jc.ErrorIsNil)

	select {
	case <-result.Complete():
	case <-time.After(veryShortTime):
		c.Fatal("publish did not complete")
	}

	c.Check(w.fromMap, jc.DeepEquals, []string{message})
	c.Check(w.fromStruct, jc.DeepEquals, []string{message})
}
