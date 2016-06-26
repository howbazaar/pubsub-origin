// Copyright 2016 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package pubsub

import (
	"encoding/json"
	"reflect"

	"github.com/juju/errors"
	"github.com/juju/loggo"
)

type structuredHub struct {
	simplehub

	marshaller  Marshaller
	annotations map[string]interface{}
	postProcess func(map[string]interface{}) (map[string]interface{}, error)
}

// Marshaller defines the Marshal and Unmarshal methods used to serialize and
// deserialize the structures used in Publish and Subscription handlers of the
// structured hub.
type Marshaller interface {
	Marshal(interface{}) ([]byte, error)
	Unmarshal([]byte, interface{}) error
}

// StructuredHubConfig is the argument struct for NewStructuredHub.
type StructuredHubConfig struct {
	// Marshaller defines how the structured hub will convert from structures to
	// a map[string]interface{} and back. If this is not specified, the
	// `JSONMarshaller` is used.
	Marshaller Marshaller

	// Annotations are added to each message that is published if and only if
	// the values are not already set.
	Annotations map[string]interface{}

	// PostProcess allows the caller to modify the resulting
	// map[string]interface{}.
	PostProcess func(map[string]interface{}) (map[string]interface{}, error)
}

// JSONMarshaller simply wraps the json.Marshal and json.Unmarshal calls for the
// Marshaller interface.
var JSONMarshaller = &jsonMarshaller{}

type jsonMarshaller struct{}

func (*jsonMarshaller) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (*jsonMarshaller) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// NewStructuredHub returns a new Hub instance.
func NewStructuredHub(config *StructuredHubConfig) Hub {
	if config == nil {
		config = new(StructuredHubConfig)
	}
	if config.Marshaller == nil {
		config.Marshaller = JSONMarshaller
	}
	return &structuredHub{
		simplehub: simplehub{
			logger: loggo.GetLogger("pubsub.structured"),
		},
		marshaller:  config.Marshaller,
		annotations: config.Annotations,
		postProcess: config.PostProcess,
	}
}

// Publish implements Hub.
func (h *structuredHub) Publish(topic Topic, data interface{}) (Completer, error) {
	asMap, err := h.toStringMap(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for key, defaultValue := range h.annotations {
		if value, exists := asMap[key]; !exists || value == reflect.Zero(reflect.TypeOf(value)).Interface() {
			asMap[key] = defaultValue
		}
	}
	if h.postProcess != nil {
		asMap, err = h.postProcess(asMap)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	h.logger.Tracef("publish %q: %#v", topic, asMap)
	return h.simplehub.Publish(topic, asMap)
}

func (h *structuredHub) toStringMap(data interface{}) (map[string]interface{}, error) {
	var result map[string]interface{}
	resultType := reflect.TypeOf(result)
	dataType := reflect.TypeOf(data)
	if dataType.AssignableTo(resultType) {
		cast, ok := data.(map[string]interface{})
		if !ok {
			return nil, errors.Errorf("%T assignable to map[string]interface{} but isn't one?", data)
		}
		return cast, nil
	}
	bytes, err := h.marshaller.Marshal(data)
	if err != nil {
		return nil, errors.Annotate(err, "marshalling")
	}
	err = h.marshaller.Unmarshal(bytes, &result)
	if err != nil {
		return nil, errors.Annotate(err, "unmarshalling")
	}
	return result, nil
}

// Subscribe implements Hub.
func (h *structuredHub) Subscribe(matcher TopicMatcher, handler interface{}) (Unsubscriber, error) {
	callback, err := newStructuredCallback(h.marshaller, handler)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return h.simplehub.Subscribe(matcher, callback.handler)
}

func toHanderType(marshaller Marshaller, rt reflect.Type, data map[string]interface{}) (reflect.Value, error) {
	mapType := reflect.TypeOf(data)
	if mapType == rt {
		return reflect.ValueOf(data), nil
	}
	sv := reflect.New(rt) // returns a Value containing *StructType
	bytes, err := marshaller.Marshal(data)
	if err != nil {
		return reflect.Indirect(sv), errors.Annotate(err, "marshalling data")
	}
	err = marshaller.Unmarshal(bytes, sv.Interface())
	if err != nil {
		return reflect.Indirect(sv), errors.Annotate(err, "unmarshalling data")
	}
	return reflect.Indirect(sv), nil
}

// checkStructuredHandler makes sure that the handler is a function that takes
// a Topic, a structure, and an error. Returns the reflect.Type for the
// structure.
func checkStructuredHandler(handler interface{}) (reflect.Type, error) {
	mapType := reflect.TypeOf(map[string]interface{}{})
	t := reflect.TypeOf(handler)
	if t.Kind() != reflect.Func {
		return nil, errors.NotValidf("handler of type %T", handler)
	}
	if t.NumIn() != 3 || t.NumOut() != 0 {
		return nil, errors.NotValidf("incorrect handler signature")
	}
	var topic Topic
	var topicType = reflect.TypeOf(topic)

	arg1 := t.In(0)
	arg2 := t.In(1)
	arg3 := t.In(2)
	if arg1 != topicType {
		return nil, errors.NotValidf("incorrect handler signature, first arg should be a pubsub.Topic")
	}
	if arg2.Kind() != reflect.Struct && arg2 != mapType {
		return nil, errors.NotValidf("incorrect handler signature, second arg should be a structure for data")
	}
	if arg3.Kind() != reflect.Interface || arg3.Name() != "error" {
		return nil, errors.NotValidf("incorrect handler signature, third arg should be error for deserialization errors")
	}
	return arg2, nil
}
