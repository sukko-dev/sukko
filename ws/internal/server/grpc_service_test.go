package server

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	serverv1 "github.com/sukko-dev/sukko/gen/proto/sukko/server/v1"
	serverstats "github.com/sukko-dev/sukko/internal/server/stats"
)

func TestGRPCService_Publish_MissingChannel(t *testing.T) {
	t.Parallel()

	svc := &GRPCService{
		servers: []*Server{{}},
		logger:  zerolog.Nop(),
	}

	_, err := svc.Publish(context.Background(), &serverv1.PublishRequest{
		Channel: "",
		Data:    []byte("test"),
	})
	if err == nil {
		t.Fatal("expected error for missing channel")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}
}

func TestGRPCService_Publish_MissingData(t *testing.T) {
	t.Parallel()

	svc := &GRPCService{
		servers: []*Server{{}},
		logger:  zerolog.Nop(),
	}

	_, err := svc.Publish(context.Background(), &serverv1.PublishRequest{
		Channel: "test.ch",
		Data:    nil,
	})
	if err == nil {
		t.Fatal("expected error for missing data")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}
}

func TestGRPCService_Subscribe_MissingChannels(t *testing.T) {
	t.Parallel()

	svc := &GRPCService{
		servers: []*Server{{}},
		logger:  zerolog.Nop(),
	}

	err := svc.Subscribe(&serverv1.SubscribeRequest{
		Channels: nil,
	}, nil)
	if err == nil {
		t.Fatal("expected error for missing channels")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}
}

func TestGRPCService_SelectServer_SingleShard(t *testing.T) {
	t.Parallel()

	server := &Server{}
	svc := &GRPCService{
		servers: []*Server{server},
		logger:  zerolog.Nop(),
	}

	selected := svc.selectServer()
	if selected != server {
		t.Error("single shard should return the one server")
	}
}

func TestGRPCService_SelectServer_MultiShard(t *testing.T) {
	t.Parallel()

	s1 := &Server{stats: serverstats.NewStats()}
	s2 := &Server{stats: serverstats.NewStats()}
	s1.stats.CurrentConnections.Store(100)
	s2.stats.CurrentConnections.Store(50)

	svc := &GRPCService{
		servers: []*Server{s1, s2},
		logger:  zerolog.Nop(),
	}

	selected := svc.selectServer()
	if selected != s2 {
		t.Error("should select least-loaded shard (s2 with 50 connections)")
	}
}
