package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/utilitywarehouse/go-operational/op"
)

var revision = "" // set during build

var (
	sinkCounterOpts = prometheus.CounterOpts{
		Name: "messages_sinked_total",
		Help: "How many messages got sinked",
	}
	sourceCounterOpts = prometheus.CounterOpts{
		Name: "messages_consumed_total",
		Help: "How many messages got consumed",
	}
)

func newOpStatus(serverPort int) *op.Status {
	return op.NewStatus("Proximo", "Interoperable GRPC based publish/subscribe").
		AddOwner("uw-labs", "#substrate-dev").
		SetRevision(revision).
		ReadyUseHealthCheck().
		AddChecker("grpc", newServerOpChecker(fmt.Sprintf("localhost:%v", serverPort)))
}

func startOperationalServer(port int, opStatus *op.Status) {
	http.Handle("/__/", op.NewHandler(opStatus))
	log.Printf("Operational server running at port %v", port)

	go func() {
		http.ListenAndServe(fmt.Sprintf(":%v", port), nil) // nolint:errcheck
	}()
}

// newServerOpChecker returns a health checker that checks the health status of the gRPC server
func newServerOpChecker(addr string) func(*op.CheckResponse) {
	var (
		unhealthyAction = "check grpc server is healthy"
		unhealthyImpact = "clients unable to call service"
	)

	return func(cr *op.CheckResponse) {
		opts := []grpc.DialOption{
			grpc.WithBlock(),
			grpc.WithInsecure(),
			grpc.WithUserAgent("grpc_health_probe"),
			grpc.WithUnaryInterceptor(grpc_middleware.ChainUnaryClient(
				grpc_retry.UnaryClientInterceptor(
					grpc_retry.WithPerRetryTimeout(time.Second),
					grpc_retry.WithMax(5),
					grpc_retry.WithCodes(codes.Unknown, codes.DeadlineExceeded, codes.Internal, codes.Unavailable),
				),
			)),
		}
		conn, err := grpc.Dial(addr, opts...)
		if err != nil {
			cr.Unhealthy(err.Error(), unhealthyAction, unhealthyImpact)
			return
		}
		defer conn.Close()

		client := healthpb.NewHealthClient(conn)
		resp, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "health"})
		if err != nil {
			cr.Unhealthy(err.Error(), unhealthyAction, unhealthyImpact)
			return
		}
		if resp.Status != healthpb.HealthCheckResponse_SERVING {
			cr.Unhealthy(fmt.Sprintf("service health status %s", resp.Status), unhealthyAction, unhealthyImpact)
			return
		}

		cr.Healthy("service is healthy")
	}
}
