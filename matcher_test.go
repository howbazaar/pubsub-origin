// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package pubsub_test

import (
	"github.com/juju/pubsub"
	"github.com/juju/testing"
	jc "github.com/juju/testing/checkers"
	gc "gopkg.in/check.v1"
)

type MatcherSuite struct {
	testing.LoggingCleanupSuite
}

var (
	_ = gc.Suite(&MatcherSuite{})
)

func (*MatcherSuite) TestTopicMatches(c *gc.C) {
	var (
		first   pubsub.Topic        = "first"
		second  pubsub.Topic        = "second"
		matcher pubsub.TopicMatcher = first
	)
	c.Assert(matcher.Match(first), jc.IsTrue)
	c.Assert(matcher.Match(second), jc.IsFalse)
}
