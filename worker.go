package durableq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/unilinq/durableq/internal/runtime"
	"github.com/unilinq/durableq/storage"
)

var (
	contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType   = reflect.TypeOf((*error)(nil)).Elem()
	bytesType   = reflect.TypeOf([]byte(nil))
	itemType    = reflect.TypeOf(storage.Item{})
	rawType     = reflect.TypeOf(json.RawMessage(nil))
)

// makeHandler adapts a user handler to the runtime contract. The signature is
// checked at registration so a mistake surfaces at wiring time rather than on
// the first item at three in the morning.
func makeHandler(handler any) (runtime.Handler, error) {
	if handler == nil {
		return nil, errors.New("durableq: handler is nil")
	}
	if h, ok := handler.(runtime.Handler); ok {
		return h, nil
	}
	v := reflect.ValueOf(handler)
	t := v.Type()
	if t.Kind() != reflect.Func {
		return nil, fmt.Errorf("durableq: handler must be a function, got %s", t)
	}
	if t.NumIn() != 2 || t.NumOut() != 1 ||
		t.In(0) != contextType || t.Out(0) != errorType {
		return nil, fmt.Errorf(
			"durableq: handler must be func(context.Context, T) error, got %s", t)
	}
	argType := t.In(1)

	return func(ctx context.Context, item storage.Item) (runtime.HandlerResult, error) {
		arg, err := decodeArg(argType, item)
		if err != nil {
			return runtime.HandlerResult{}, err
		}
		out := v.Call([]reflect.Value{reflect.ValueOf(ctx), arg})
		if e, _ := out[0].Interface().(error); e != nil {
			return runtime.HandlerResult{}, e
		}
		return runtime.HandlerResult{}, nil
	}, nil
}

// decodeArg turns an item's payload into the handler's argument.
//
// A payload that no longer decodes is an ordinary failure: it is retried under
// the item's policy and dead-lettered when the attempts run out. It is never
// retried for ever, which is what makes a worker-version skew survivable.
func decodeArg(argType reflect.Type, item storage.Item) (reflect.Value, error) {
	switch argType {
	case itemType:
		return reflect.ValueOf(item), nil
	case bytesType, rawType:
		return reflect.ValueOf(item.Payload).Convert(argType), nil
	}
	ptr := reflect.New(argType)
	if err := json.Unmarshal(item.Payload, ptr.Interface()); err != nil {
		return reflect.Value{}, fmt.Errorf(
			"durableq: decoding payload into %s: %w", argType, err)
	}
	return ptr.Elem(), nil
}
