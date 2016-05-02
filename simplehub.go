// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package pubsub

import (
	"regexp"
	"sync"

	"github.com/juju/errors"
	"github.com/juju/loggo"
)

var logger = loggo.GetLogger("pubsub")

type simplehub struct {
	mutex       sync.Mutex
	subscribers []*subscriber
	idx         int
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
	count := copy(dupe, h.subscribers)
	logger.Debugf("duplicated %d subscribers", count)
	return dupe
}

func (s *subscriber) matchTopic(topic string) bool {
	logger.Debugf("matchTopic: %s, %s", s.topic.String(), topic)
	return s.topic.MatchString(topic)
}

func (h *simplehub) Publish(topic string, data interface{}) Completer {
	done := make(chan struct{})
	subs := h.dupeSubscribers()
	wait := sync.WaitGroup{}

	go func() {
		for _, s := range subs {
			if s.matchTopic(topic) {
				logger.Debugf("calling handler")
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

	return &doneHandle{done: done}
}

func (h *simplehub) Subscribe(topic string, handler func(topic string, data interface{})) (Unsubscriber, error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	id := h.idx
	h.idx++

	matcher, err := regexp.Compile(topic)
	if err != nil {
		return nil, errors.Annotate(err, "topic should be a regex string")
	}

	h.subscribers = append(h.subscribers, &subscriber{
		id:      id,
		topic:   matcher,
		handler: handler,
	})

	return &handle{hub: h, id: id}, nil
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
