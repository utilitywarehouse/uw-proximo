# uw-proximo [![Docker Repository on Quay](https://quay.io/repository/utilitywarehouse/uw-proximo/status "Docker Repository on Quay")](https://quay.io/repository/utilitywarehouse/uw-proximo) [![CircleCI](https://circleci.com/gh/utilitywarehouse/uw-proximo.svg?style=svg)](https://circleci.com/gh/utilitywarehouse/uw-proximo)
Utility Warehouse specific version of https://github.com/uw-labs/proximo.
This version exposes prometheus metrics endpoint at `/__/metrics` on port 8080 
(can be configured via probe-port option) and health check endpoint that checks health of the gRPC server and 
backend connections at `/__/health` on the same port.

```
Usage: proximo [OPTIONS] COMMAND [arg...]

GRPC Proxy gateway for message queue systems
                            
Options:                    
      --port                Port to listen on (env $PROXIMO_PORT) (default 6868)
      --probe-port          Port to listen for healtcheck requests (env $PROXIMO_PROBE_PORT) (default 8080)
      --max-failed-checks   Maximum number or consecutively failed health checks for a single connection. (env $PROXIMO_MAX_FAILED_CHECKS) (default 3)
      --endpoints           The proximo endpoints to expose (consume, publish) (env $PROXIMO_ENDPOINTS) (default "consume,publish")
                            
Commands:                   
  kafka                     Use kafka backend
  nats-streaming            Use NATS streaming backend
  mem                       Use in-memory testing backend
                            
Run 'proximo COMMAND --help' for more information on a command.
```