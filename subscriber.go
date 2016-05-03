// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package pubsub

import (
	"reflect"
	"regexp"
	"sync"

	"github.com/juju/errors"
	"github.com/juju/utils/deque"
)

type subscriber struct {
	id int

	topic   *regexp.Regexp
	handler func(topic string, data interface{})

	mutex   sync.Mutex
	pending *deque.Deque
	closed  chan struct{}
	data    chan struct{}
	done    chan struct{}
}

func newSubscriber(topic string, handler interface{}) (*subscriber, error) {
	matcher, err := regexp.Compile(topic)
	if err != nil {
		return nil, errors.Annotate(err, "topic should be a regex string")
	}
	if handler == nil {
		return nil, errors.NotValidf("missing handler")
	}
	f, err := checkHandler(handler)
	if err != nil {
		return nil, errors.Trace(err)
	}
	closed := make(chan struct{})
	close(closed)
	sub := &subscriber{
		topic:   matcher,
		handler: f,
		pending: deque.New(),
		data:    make(chan struct{}, 1),
		done:    make(chan struct{}),
		closed:  closed,
	}
	go sub.loop()
	return sub, nil
}

func (s *subscriber) close() {
	// need to iterate through all the pending calls and make sure the wait group
	// is decremented. this isn't exposed yet, but needs to be.
	close(s.done)
}

func (s *subscriber) loop() {
	var next <-chan struct{}
	for {
		select {
		case <-s.done:
			return
		case <-s.data:
			// Has new data been pushed on?
		case <-next:
			// If there was already data, next is a closed channel.
			// otherwise it is nil so won't pass through.
		}
		call, empty := s.popOne()
		if empty {
			next = nil
		} else {
			next = s.closed
		}
		// call *should* never be nil as we should only be calling
		// popOne in the situations where there is actually something to pop.
		if call != nil {
			call()
		}
	}
}

func (s *subscriber) popOne() (func(), bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	val, ok := s.pending.PopFront()
	if !ok {
		// nothing to do
		return nil, true
	}
	empty := s.pending.Len() == 0
	return val.(func()), empty
}

func (s *subscriber) notify(call func()) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.pending.PushBack(call)
	if s.pending.Len() == 1 {
		s.data <- struct{}{}
	}
}

func checkHandler(handler interface{}) (func(string, interface{}), error) {
	t := reflect.TypeOf(handler)
	if t.Kind() != reflect.Func {
		return nil, errors.NotValidf("handler of type %T", handler)
	}
	var result func(string, interface{})
	rt := reflect.TypeOf(result)
	if !t.AssignableTo(rt) {
		return nil, errors.NotValidf("incorrect handler signature")
	}
	f, ok := handler.(func(string, interface{}))
	if !ok {
		// This shouldn't happen due to the assignable check just above.
		return nil, errors.NotValidf("incorrect handler signature")
	}
	return f, nil
}
