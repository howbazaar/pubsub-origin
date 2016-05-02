// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package pubsub

import (
	"reflect"

	"github.com/juju/errors"
	"github.com/juju/loggo"
	"gopkg.in/yaml.v2"
)

type structuredHub struct {
	simplehub
}

// NewStructuredHub returns a new Hub instance.
//
// A structured hub serializes the data through an intermediate format.
// In this case, YAML.
func NewStructuredHub() Hub {
	return &structuredHub{
		simplehub{
			logger: loggo.GetLogger("pubsub.structured"),
		}}
}

// Publish implements Hub.
func (h *structuredHub) Publish(topic string, data interface{}) (Completer, error) {
	var bytes []byte
	// TODO: publish must be a struct
	if data != nil {
		var err error
		bytes, err = yaml.Marshal(data)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return h.simplehub.Publish(topic, bytes)
}

// Subscribe implements Hub.
func (h *structuredHub) Subscribe(topic string, handler interface{}) (Unsubscriber, error) {
	rt, err := h.checkHandler(handler)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f := reflect.ValueOf(handler)
	// Wrap the hander func in something that deserializes the YAML into the structure expected.
	deserialize := func(t string, data interface{}) {
		bytes, ok := data.([]byte)
		if !ok {
			h.logger.Warningf("bad publish data: %v", data)
			return
		}
		sv := reflect.New(rt) // returns a Value containing *StructType
		err := yaml.Unmarshal(bytes, sv.Interface())
		if err != nil {
			h.logger.Errorf("bad publish data: %v", err)
			return
		}
		args := []reflect.Value{reflect.ValueOf(t), reflect.Indirect(sv)}
		f.Call(args)
	}
	return h.simplehub.Subscribe(topic, deserialize)
}

// checkHandler makes sure that the handler is a function that takes a string and
// a structure. Returns the reflect.Type for the structure.
func (h *structuredHub) checkHandler(handler interface{}) (reflect.Type, error) {
	t := reflect.TypeOf(handler)
	if t.Kind() != reflect.Func {
		return nil, errors.NotValidf("handler of type %T", handler)
	}
	if t.NumIn() != 2 || t.NumOut() != 0 {
		return nil, errors.NotValidf("incorrect handler signature")
	}
	arg1 := t.In(0)
	arg2 := t.In(1)
	if arg1.Kind() != reflect.String || arg2.Kind() != reflect.Struct {
		return nil, errors.NotValidf("incorrect handler signature")
	}
	return arg2, nil
}
