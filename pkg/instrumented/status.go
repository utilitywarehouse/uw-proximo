package instrumented

import (
	"strings"

	"github.com/utilitywarehouse/go-operational/op"
	"github.com/uw-labs/substrate"
)

func newOpsCheck(c substrate.Statuser, impact string) func(ch *op.CheckResponse) {
	const action = "Troubleshoot pubsub"
	return func(ch *op.CheckResponse) {
		s, err := c.Status()
		if err != nil {
			ch.Unhealthy("Can't get pubsub status: "+err.Error(), action, impact)
			return
		}

		problems := strings.Join(s.Problems, ". ")
		switch s.Working {
		case false:
			ch.Unhealthy(problems, action, impact)
		case true && len(s.Problems) > 0:
			ch.Degraded(problems, action)
		default:
			ch.Healthy("pubsub is reachable")
		}
	}
}
