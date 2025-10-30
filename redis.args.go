package redisqueue

import (
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type XPendingExtArgs redis.XPendingExtArgs

func (e XPendingExtArgs) Format(f fmt.State, verb rune) {
	databytes, _ := json.Marshal(e)
	_, _ = f.Write(databytes)
}

type XClaimArgs redis.XClaimArgs

func (e XClaimArgs) Format(f fmt.State, verb rune) {
	databytes, _ := json.Marshal(e)
	_, _ = f.Write(databytes)
}

type XReadGroupArgs redis.XReadGroupArgs

func (e XReadGroupArgs) Format(f fmt.State, verb rune) {
	databytes, _ := json.Marshal(e)
	_, _ = f.Write(databytes)
}
