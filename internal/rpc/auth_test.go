package rpc

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestUnaryServerAuth(t *testing.T) {
	t.Parallel()
	interceptor := UnaryServerAuth("current", "previous")
	handler := func(context.Context, any) (any, error) { return "ok", nil }
	tests := []struct {
		name, header string
		want         codes.Code
	}{{"current", "Bearer current", codes.OK}, {"previous", "Bearer previous", codes.OK}, {"missing", "", codes.Unauthenticated}, {"wrong", "Bearer wrong", codes.Unauthenticated}}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if tt.header != "" {
				ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(authorization, tt.header))
			}
			_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, handler)
			if got := status.Code(err); got != tt.want {
				t.Fatalf("got %s want %s", got, tt.want)
			}
		})
	}
}
func TestUnaryRecovery(t *testing.T) {
	t.Parallel()
	interceptor := UnaryRecovery()
	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test/Panic"}, func(context.Context, any) (any, error) { panic("boom") })
	if status.Code(err) != codes.Internal {
		t.Fatalf("got %s", status.Code(err))
	}
}

func TestMatchesBearerToken(t *testing.T) {
	t.Parallel()
	if !matchesBearerToken("Bearer service-token", "service-token") {
		t.Fatal("valid service token rejected")
	}
	for _, header := range []string{"service-token", "Bearer service-token-extra", "Bearer service-toke", ""} {
		if matchesBearerToken(header, "service-token") {
			t.Fatalf("invalid service token accepted: %q", header)
		}
	}
}
