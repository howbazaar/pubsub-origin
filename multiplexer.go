// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package pubsub

import (
	"reflect"
	"sync"

	"github.com/juju/errors"
)

// Multiplexer allows multiple subscriptions to be made sharing a single
// message queue from the hub. This means that all the messages for the
// various subscriptions are called back in the order that the messages were
// published. If more than on handler is added to the Multiplexer that matches
// any given topic, the handlers are called back one after the other in the
// order that they were added.
type Multiplexer interface {
	TopicMatcher
	Add(matcher TopicMatcher, handler interface{}) error
}

type element struct {
	matcher  TopicMatcher
	callback *structuredCallback
}

type multiplexer struct {
	mu         sync.Mutex
	outputs    []element
	marshaller Marshaller
}

// NewMultiplexer creates a new multiplexer for the hub and subscribes it.
// Unsubscribing the multiplexer stops calls for all handlers added.
// Only structured hubs support multiplexer.
func NewMultiplexer(hub Hub) (Unsubscriber, Multiplexer, error) {
	shub, ok := hub.(*structuredHub)
	if !ok {
		return nil, nil, errors.New("hub was not a StructuredHub")
	}
	mp := &multiplexer{marshaller: shub.marshaller}
	unsub, err := hub.Subscribe(mp, mp.callback)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return unsub, mp, nil
}

// Add another topic matcher and handler to the multiplexer.
func (m *multiplexer) Add(matcher TopicMatcher, handler interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	callback, err := newStructuredCallback(m.marshaller, handler)
	if err != nil {
		return errors.Trace(err)
	}
	m.outputs = append(m.outputs, element{matcher: matcher, callback: callback})
	return nil
}

func (m *multiplexer) callback(topic Topic, data map[string]interface{}, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Should never error here.
	if err != nil {
		logger.Errorf("multiplexer callback err: %v", err)
		return
	}
	for _, element := range m.outputs {
		if element.matcher.Match(topic) {
			element.callback.handler(topic, data)
		}
	}
}

// Match implements TopicMatcher. If any of the topic matchers added for the
// handlers match the topic, the multiplexer matches.
func (m *multiplexer) Match(topic Topic) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, element := range m.outputs {
		if element.matcher.Match(topic) {
			return true
		}
	}
	return false
}

type structuredCallback struct {
	marshaller Marshaller
	callback   reflect.Value
	dataType   reflect.Type
}

func newStructuredCallback(marshaller Marshaller, handler interface{}) (*structuredCallback, error) {
	rt, err := checkStructuredHandler(handler)
	if err != nil {
		return nil, errors.Trace(err)
	}
	logger.Tracef("new structured callback, return type %v", rt)
	return &structuredCallback{
		marshaller: marshaller,
		callback:   reflect.ValueOf(handler),
		dataType:   rt,
	}, nil
}

func (s *structuredCallback) handler(topic Topic, data interface{}) {
	var (
		err   error
		value reflect.Value
	)
	asMap, ok := data.(map[string]interface{})
	if !ok {
		err = errors.Errorf("bad data: %v", data)
		value = reflect.Indirect(reflect.New(s.dataType))
	} else {
		logger.Tracef("convert map to %v", s.dataType)
		value, err = toHanderType(s.marshaller, s.dataType, asMap)
	}
	// NOTE: you can't just use reflect.ValueOf(err) as that doesn't work
	// with nil errors. reflect.ValueOf(nil) isn't a valid value. So we need
	// to make  sure that we get the type of the parameter correct, which is
	// the error interface.
	errValue := reflect.Indirect(reflect.ValueOf(&err))
	args := []reflect.Value{reflect.ValueOf(topic), value, errValue}
	s.callback.Call(args)
}
