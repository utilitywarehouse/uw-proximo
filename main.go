package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	cli "github.com/jawher/mow.cli"
	stan "github.com/nats-io/stan.go"
	"github.com/pkg/errors"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"

	"github.com/uw-labs/proximo"
	"github.com/uw-labs/proximo/backend/kafka"
	"github.com/uw-labs/proximo/backend/mem"
	"github.com/uw-labs/proximo/backend/natsstreaming"
	"github.com/uw-labs/proximo/proto"

	"github.com/utilitywarehouse/uw-proximo/pkg/instrumented"
)

const (
	consumeEndpoint = "consume"
	publishEndpoint = "publish"
)

func main() {
	var (
		sourceFactory proximo.AsyncSourceFactory
		sinkFactory   proximo.AsyncSinkFactory
		enabled       map[string]bool
	)

	app := cli.App("proximo", "GRPC Proxy gateway for message queue systems")

	port := app.Int(cli.IntOpt{
		Name:   "port",
		Value:  6868,
		Desc:   "Port to listen on",
		EnvVar: "PROXIMO_PORT",
	})

	probePort := app.Int(cli.IntOpt{
		Name:   "probe-port",
		Value:  8080,
		Desc:   "Port to listen for healtcheck requests",
		EnvVar: "PROXIMO_PROBE_PORT",
	})

	maxFailedChecks := app.Int(cli.IntOpt{
		Name:   "max-failed-checks",
		Value:  3,
		Desc:   "Maximum number or consecutively failed health checks for a single connection.",
		EnvVar: "PROXIMO_MAX_FAILED_CHECKS",
	})

	endpoints := app.String(cli.StringOpt{
		Name:   "endpoints",
		Value:  fmt.Sprintf("%s,%s", consumeEndpoint, publishEndpoint),
		Desc:   "The proximo endpoints to expose (consume, publish)",
		EnvVar: "PROXIMO_ENDPOINTS",
	})

	app.Before = func() {
		enabled = parseEndpoints(*endpoints)
	}

	app.Command("kafka", "Use kafka backend", func(cmd *cli.Cmd) {
		brokerString := cmd.String(cli.StringOpt{
			Name:   "brokers",
			Value:  "localhost:9092",
			Desc:   "Broker addresses e.g., \"server1:9092,server2:9092\"",
			EnvVar: "PROXIMO_KAFKA_BROKERS",
		})
		kafkaVersion := cmd.String(cli.StringOpt{
			Name:   "version",
			Desc:   "Kafka Version e.g. 1.1.1, 0.10.2.0",
			EnvVar: "PROXIMO_KAFKA_VERSION",
		})

		cmd.Action = func() {
			brokers := strings.Split(*brokerString, ",")

			if enabled[consumeEndpoint] {
				sourceFactory = &kafka.AsyncSourceFactory{
					Brokers: brokers,
					Version: *kafkaVersion,
				}
			}
			if enabled[publishEndpoint] {
				sinkFactory = &kafka.AsyncSinkFactory{
					Brokers: brokers,
					Version: *kafkaVersion,
				}
			}

			log.Printf("Using kafka at %s\n", brokers)
		}
	})

	app.Command("nats-streaming", "Use NATS streaming backend", func(cmd *cli.Cmd) {
		url := cmd.String(cli.StringOpt{
			Name:   "url",
			Value:  "nats://localhost:4222",
			Desc:   "NATS url",
			EnvVar: "PROXIMO_NATS_URL",
		})
		cid := cmd.String(cli.StringOpt{
			Name:   "cid",
			Value:  "test-cluster",
			Desc:   "cluster id",
			EnvVar: "PROXIMO_NATS_CLUSTER_ID",
		})
		maxInflight := cmd.Int(cli.IntOpt{
			Name:   "max-inflight",
			Value:  stan.DefaultMaxInflight,
			Desc:   "maximum number of unacknowledged messages",
			EnvVar: "PROXIMO_NATS_MAX_INFLIGHT",
		})
		pingIntervalSeconds := cmd.Int(cli.IntOpt{
			Name:   "ping-interval",
			Value:  3,
			Desc:   "interval in seconds for connection pings",
			EnvVar: "PROXIMO_NATS_PING_INTERVAL_SECONDS",
		})
		pingNumTimeouts := cmd.Int(cli.IntOpt{
			Name:   "num-ping-timeouts",
			Value:  5,
			Desc:   "number of pings to time out before connection considered broken",
			EnvVar: "PROXIMO_NATS_NUM_PING_TIMEOUTS",
		})
		cmd.Action = func() {
			if enabled[consumeEndpoint] {
				sourceFactory = natsstreaming.AsyncSourceFactory{
					URL:                    *url,
					ClusterID:              *cid,
					MaxInflight:            *maxInflight,
					ConnectionNumPings:     *pingNumTimeouts,
					ConnectionPingInterval: *pingIntervalSeconds,
				}
			}
			if enabled[publishEndpoint] {
				sinkFactory = natsstreaming.AsyncSinkFactory{
					URL:       *url,
					ClusterID: *cid,
				}
			}

			log.Printf("Using NATS streaming server at %s with cluster id %s and max inflight %v\n", *url, *cid, *maxInflight)
		}
	})

	app.Command("mem", "Use in-memory testing backend", func(cmd *cli.Cmd) {
		cmd.Action = func() {
			h := mem.NewBackend()

			if enabled[consumeEndpoint] {
				sourceFactory = h
			}
			if enabled[publishEndpoint] {
				sinkFactory = h
			}

			log.Printf("Using in memory testing backend")
		}
	})

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
	if err := listenAndServe(sourceFactory, sinkFactory, *port, *probePort, *maxFailedChecks); err != nil {
		log.Fatal(err)
	}
	log.Println("Server terminated cleanly")
}

func parseEndpoints(endpoints string) map[string]bool {
	enabled := make(map[string]bool, 2)

	for _, endpoint := range strings.Split(endpoints, ",") {
		switch endpoint {
		case consumeEndpoint, publishEndpoint:
			log.Printf("%s endpoint enabled\n", endpoint)
			enabled[endpoint] = true
		default:
			log.Fatalf("invalid expose-endpoint flag: %s", endpoint)
		}
	}

	return enabled
}

func listenAndServe(sourceFactory proximo.AsyncSourceFactory, sinkFactory proximo.AsyncSinkFactory, port, probePort, maxFailedChecks int) error {
	opStatus := newOpStatus(port)
	backendChecker := instrumented.NewBackendStatusChecker(maxFailedChecks)
	opStatus.AddChecker("backend", backendChecker.CheckStatus)
	startOperationalServer(probePort, opStatus)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return errors.Wrap(err, "failed to listen")
	}
	defer lis.Close()

	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 5 * time.Minute,
		}),
		grpc_middleware.WithUnaryServerChain(
			grpc_prometheus.UnaryServerInterceptor,
			grpc_recovery.UnaryServerInterceptor(),
		),
		grpc_middleware.WithStreamServerChain(
			grpc_prometheus.StreamServerInterceptor,
			grpc_recovery.StreamServerInterceptor(),
		),
	}
	grpcServer := grpc.NewServer(opts...)
	defer grpcServer.Stop()
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("health", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthSrv)

	if sinkFactory != nil {
		sinkFactory = instrumented.AsyncSinkFactory{
			BackendStatusChecker: backendChecker,
			CounterOpts:          sinkCounterOpts,
			SinkFactory:          sinkFactory,
		}
		proto.RegisterMessageSinkServer(grpcServer, &proximo.SinkServer{SinkFactory: sinkFactory})
	}
	if sourceFactory != nil {
		sourceFactory = instrumented.AsyncSourceFactory{
			BackendStatusChecker: backendChecker,
			CounterOpts:          sourceCounterOpts,
			SourceFactory:        sourceFactory,
		}
		proto.RegisterMessageSourceServer(grpcServer, &proximo.SourceServer{SourceFactory: sourceFactory})
	}

	errCh := make(chan error, 1)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() { errCh <- grpcServer.Serve(lis) }()
	select {
	case err := <-errCh:
		return errors.Wrap(err, "failed to serve grpc")
	case <-sigCh:
		return nil
	}
}
