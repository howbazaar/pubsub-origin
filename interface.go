// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package pubsub

// SimpleHub represents an in-process delivery mechanism. The hub maintains a
// list of topic subscribers. The data is passed through untouched.
type SimpleHub interface {

	// Publish will notifiy all the subscribers that are interested by calling
	// their handler function.
	Publish(topic string, data interface{}) (Completer, error)

	// Subscribe takes a topic regular expression, and a handler function.
	// If the topicRegex is not a valid regular expression, and error is returned.
	Subscribe(topicRegex string, handler func(topic string, data interface{})) (Unsubscriber, error)
}

// Completer provides a way for the caller of publish to know when all of the
// subscribers have finished being notified.
type Completer interface {
	// Complete returns a channel that is closed when all the subscribers
	// have been notified of the event.
	Complete() <-chan struct{}
}

// Unsubscriber provides a simple way to Unsubscribe.
type Unsubscriber interface {
	Unsubscribe()
}
