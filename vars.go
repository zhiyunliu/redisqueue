package redisqueue

import (
	"context"
	"regexp"
	"strings"

	"github.com/hashicorp/go-version"
	"github.com/redis/go-redis/v9"
)

func checkXPendingEnable(redisClient redis.UniversalClient) (bool, error) {
	info, err := redisClient.Info(context.Background(), "server").Result()
	if err != nil {
		return false, err
	}
	rdsServerVer := ""
	rdsVerReg := regexp.MustCompile(`redis_version:(.+)`)
	match := rdsVerReg.FindAllStringSubmatch(info, -1)
	if len(match) >= 1 {
		rdsServerVer = strings.TrimSpace(match[0][1]) // 获取版本号
	}

	// enableXPendingExtIdele enable XPENDING EXTIDLE(支持redis 6.2.0以上版本)
	baseVersion, _ := version.NewVersion("6.2.0")
	curVersion, _ := version.NewVersion(rdsServerVer)

	if curVersion.GreaterThan(baseVersion) { // 当前版本大于等于6.2.0
		return true, nil
	}

	return false, nil

}
