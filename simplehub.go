// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package pubsub

import (
	"reflect"
	"regexp"
	"sync"

	"github.com/juju/errors"
	"github.com/juju/loggo"
)

// NewSimpleHub returns a new Hub instance.
//
// A simple hub does not touch the data that is passed through to Publish.
// This data is passed through to each Subscriber. Note that all subscribers
// are notified in parallel, and that no modification should be done to the
// data or data races will occur.
func NewSimpleHub() Hub {
	return &simplehub{
		logger: loggo.GetLogger("pubsub.simple"),
	}
}

type simplehub struct {
	mutex       sync.Mutex
	subscribers []*subscriber
	idx         int
	logger      loggo.Logger
}

type subscriber struct {
	id int

	topic   *regexp.Regexp
	handler func(topic string, data interface{})
}

type doneHandle struct {
	done chan struct{}
}

func (d *doneHandle) Complete() <-chan struct{} {
	return d.done
}

func (h *simplehub) dupeSubscribers() []*subscriber {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	dupe := make([]*subscriber, len(h.subscribers))
	copy(dupe, h.subscribers)
	return dupe
}

func (s *subscriber) matchTopic(topic string) bool {
	return s.topic.MatchString(topic)
}

func (h *simplehub) Publish(topic string, data interface{}) (Completer, error) {
	done := make(chan struct{})
	subs := h.dupeSubscribers()
	wait := sync.WaitGroup{}

	go func() {
		for _, s := range subs {
			if s.matchTopic(topic) {
				wait.Add(1)
				go func() {
					defer wait.Done()
					s.handler(topic, data)
				}()
			}
		}
		wait.Wait()
		close(done)
	}()

	return &doneHandle{done: done}, nil
}

func (h *simplehub) Subscribe(topic string, handler interface{}) (Unsubscriber, error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	matcher, err := regexp.Compile(topic)
	if err != nil {
		return nil, errors.Annotate(err, "topic should be a regex string")
	}
	if handler == nil {
		return nil, errors.NotValidf("missing handler")
	}
	f, err := h.checkHandler(handler)
	if err != nil {
		return nil, errors.Trace(err)
	}

	id := h.idx
	h.idx++
	h.subscribers = append(h.subscribers, &subscriber{
		id:      id,
		topic:   matcher,
		handler: f,
	})

	return &handle{hub: h, id: id}, nil
}

func (h *simplehub) checkHandler(handler interface{}) (func(string, interface{}), error) {
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

func (h *simplehub) unsubscribe(id int) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	for i, sub := range h.subscribers {
		if sub.id == id {
			h.subscribers = append(h.subscribers[0:i], h.subscribers[i+1:]...)
			return
		}
	}
}

type handle struct {
	hub *simplehub
	id  int
}

func (h *handle) Unsubscribe() {
	h.hub.unsubscribe(h.id)
}
