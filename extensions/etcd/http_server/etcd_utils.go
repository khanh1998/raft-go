package http_server

import (
	"errors"
	"strconv"
	"strings"
	"time"

	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/etcd/common"

	"github.com/gin-gonic/gin"
)

func getTimeDuration(c *gin.Context, key string) (*time.Duration, error) {
	value, exist := getKey(c, key)
	if exist {
		dur, err := time.ParseDuration(value)
		if err != nil {
			return nil, err
		}
		return &dur, nil
	}
	return nil, nil
}

func getUint64(c *gin.Context, key string) (*uint64, error) {
	value, exist := getKey(c, key)
	if exist {
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return nil, err
		}
		return &num, nil
	}
	return nil, nil
}

// nil response means user don't set the key at all,
// if user set the key, they have to explicitly specify the value.
// for example, they can't provide ambiguous input like `ttl=`,
// either don't provide `ttl` at all or provide a valid value like `ttl=10000`.
func getInt(c *gin.Context, key string) (*int, error) {
	value, exist := getKey(c, key)
	if exist {
		num, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
		return &num, nil
	}
	return nil, nil
}

func getString(c *gin.Context, key string) *string {
	value, exist := getKey(c, key)
	if exist {
		return gc.GetPointer(value)
	}
	return nil
}

func getBool(c *gin.Context, key string) (*bool, error) {
	value, exist := getKey(c, key)
	if exist {
		boolean, err := strconv.ParseBool(value)
		if err != nil {
			return nil, err
		}
		return &boolean, nil
	}
	return nil, nil
}

func getKey(c *gin.Context, key string) (string, bool) {
	value, exist := c.GetPostForm(key)
	if !exist {
		value, exist := c.GetQuery(key)
		if !exist {
			return "", false
		}
		return value, true
	}
	return value, true
}

func parseDeleteRequest(c *gin.Context) (EtcdCommandRequest, error) {
	key := c.Param("key")
	if key == "" {
		return EtcdCommandRequest{}, common.CreateError("key", errors.New("`key` can't be empty"))
	}

	cmd := EtcdCommandRequest{
		Action: strings.ToLower(c.Request.Method),
		Key:    key,
	}

	var err error

	cmd.PrevExist, err = getBool(c, "prevExist")
	if err != nil {
		return EtcdCommandRequest{}, common.CreateError("prevExist", err)
	}

	cmd.PrevValue = getString(c, "prevValue")

	cmd.PrevIndex, err = getInt(c, "prevIndex")
	if err != nil {
		return EtcdCommandRequest{}, common.CreateError("prevIndex", err)
	}

	cmd.Prefix, err = getBool(c, "prefix")
	if err != nil {
		return EtcdCommandRequest{}, common.CreateError("prefix", err)
	}

	return cmd, nil
}

func parseGetRequest(c *gin.Context) (EtcdCommandRequest, error) {
	key := c.Param("key")
	if key == "" {
		return EtcdCommandRequest{}, common.CreateError("key", errors.New("`key` can't be empty"))
	}

	cmd := EtcdCommandRequest{
		Action: strings.ToLower(c.Request.Method),
		Key:    key,
	}

	var err error

	cmd.Wait, err = getBool(c, "wait")
	if err != nil {
		return EtcdCommandRequest{}, common.CreateError("wait", err)
	}

	cmd.WaitIndex, err = getInt(c, "waitIndex")
	if err != nil {
		return EtcdCommandRequest{}, common.CreateError("waitIndex", err)
	}

	cmd.Prefix, err = getBool(c, "prefix")
	if err != nil {
		return EtcdCommandRequest{}, common.CreateError("prefix", err)
	}

	return cmd, nil
}

func parsePutRequest(c *gin.Context) (EtcdCommandRequest, error) {
	key := c.Param("key")
	if key == "" {
		return EtcdCommandRequest{}, common.CreateError("key", errors.New("can't be empty"))
	}

	cmd := EtcdCommandRequest{
		Action: strings.ToLower(c.Request.Method),
		Key:    key,
	}

	var err error

	cmd.Value = getString(c, "value")

	cmd.Ttl, err = getTimeDuration(c, "ttl")
	if err != nil {
		return EtcdCommandRequest{}, common.CreateError("ttl", err)
	}

	cmd.Refresh, err = getBool(c, "refresh")
	if err != nil {
		return EtcdCommandRequest{}, common.CreateError("refresh", err)
	}

	cmd.PrevExist, err = getBool(c, "prevExist")
	if err != nil {
		return EtcdCommandRequest{}, common.CreateError("prevExist", err)
	}

	cmd.PrevValue = getString(c, "prevValue")

	cmd.PrevIndex, err = getInt(c, "prevIndex")
	if err != nil {
		return EtcdCommandRequest{}, common.CreateError("prevIndex", err)
	}

	return cmd, nil
}
