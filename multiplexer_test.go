// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package pubsub_test

import (
	"github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"

	"github.com/juju/pubsub"
)

type MultiplexerHubSuite struct {
	testing.LoggingCleanupSuite
}

var _ = gc.Suite(&MultiplexerHubSuite{})

func (*MultiplexerHubSuite) TestNewMultiplexerNil(c *gc.C) {
	sub, multi, err := pubsub.NewMultiplexer(nil)
	c.Check(sub, gc.IsNil)
	c.Check(multi, gc.IsNil)
	c.Check(err, gc.ErrorMatches, "nil hub not valid")
}

func (*MultiplexerHubSuite) TestNewMultiplexerSimpleHub(c *gc.C) {
	hub := pubsub.NewSimpleHub()
	sub, multi, err := pubsub.NewMultiplexer(hub)
	c.Check(sub, gc.IsNil)
	c.Check(multi, gc.IsNil)
	c.Check(err, gc.ErrorMatches, "hub was not a StructuredHub")
}

func (*MultiplexerHubSuite) TestNewMultiplexerStructuredHub(c *gc.C) {
	hub := pubsub.NewStructuredHub(nil)
	sub, multi, err := pubsub.NewMultiplexer(hub)
	c.Assert(err, jc.ErrorIsNil)
	sub.Unsubscribe()
	c.Check(multi, gc.NotNil)
}

func (*MultiplexerHubSuite) TestMultiplexerAdd(c *gc.C) {
	hub := pubsub.NewStructuredHub(nil)
	sub, multi, err := pubsub.NewMultiplexer(hub)
	c.Assert(err, jc.ErrorIsNil)
	sub.Unsubscribe()
	for i, test := range []struct {
		description string
		handler     interface{}
		err         string
	}{
		{
			description: "nil handler",
			err:         "nil handler not valid",
		}, {
			description: "string handler",
			handler:     "a string",
			err:         "handler of type string not valid",
		}, {
			description: "simple hub handler function",
			handler:     func(pubsub.Topic, interface{}) {},
			err:         "expected 3 args, got 2, incorrect handler signature not valid",
		}, {
			description: "bad return values in handler function",
			handler:     func(pubsub.Topic, interface{}, error) error { return nil },
			err:         "expected no return values, got 1, incorrect handler signature not valid",
		}, {
			description: "bad first arg",
			handler:     func(string, map[string]interface{}, error) {},
			err:         "first arg should be a pubsub.Topic, incorrect handler signature not valid",
		}, {
			description: "bad second arg",
			handler:     func(pubsub.Topic, string, error) {},
			err:         "second arg should be a structure for data, incorrect handler signature not valid",
		}, {
			description: "bad third arg",
			handler:     func(pubsub.Topic, map[string]interface{}, bool) {},
			err:         "third arg should be error for deserialization errors, incorrect handler signature not valid",
		}, {
			description: "accept map[string]interface{}",
			handler:     func(pubsub.Topic, map[string]interface{}, error) {},
		}, {
			description: "bad map[string]string",
			handler:     func(pubsub.Topic, map[string]string, error) {},
			err:         "second arg should be a structure for data, incorrect handler signature not valid",
		}, {
			description: "accept struct value",
			handler:     func(pubsub.Topic, Emitter, error) {},
		},
	} {
		c.Logf("test %d: %s", i, test.description)
		err := multi.Add(pubsub.MatchAll, test.handler)
		if test.err == "" {
			c.Check(err, jc.ErrorIsNil)
		} else {
			c.Check(err, gc.ErrorMatches, test.err)
		}
	}
}

func (*MultiplexerHubSuite) TestMatcher(c *gc.C) {
	hub := pubsub.NewStructuredHub(nil)
	sub, multi, err := pubsub.NewMultiplexer(hub)
	c.Assert(err, jc.ErrorIsNil)
	defer sub.Unsubscribe()

	noopFunc := func(pubsub.Topic, map[string]interface{}, error) {}
	multi.Add(first, noopFunc)
	multi.Add(pubsub.MatchRegex("second.*"), noopFunc)

	c.Check(multi.Match(first), jc.IsTrue)
	c.Check(multi.Match(firstdot), jc.IsFalse)
	c.Check(multi.Match(second), jc.IsTrue)
	c.Check(multi.Match(space), jc.IsFalse)
}
