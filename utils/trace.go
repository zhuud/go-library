package utils

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/trace"
)

type valueOnlyContext struct {
	context.Context
}

func (valueOnlyContext) Deadline() (deadline time.Time, ok bool) {
	return
}

func (valueOnlyContext) Done() <-chan struct{} {
	return nil
}

func (valueOnlyContext) Err() error {
	return nil
}

// StripContext takes all values from the given ctx, without deadline and error control.
func StripContext(ctx context.Context) context.Context {
	return valueOnlyContext{
		Context: ctx,
	}
}

func SpanIDFromContext(ctx context.Context) string {
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.HasSpanID() {
		return spanCtx.SpanID().String()
	}

	return ""
}

func TraceIDFromContext(ctx context.Context) string {
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.HasTraceID() {
		return spanCtx.TraceID().String()
	}

	return ""
}
