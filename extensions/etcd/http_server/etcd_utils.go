package http_server

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/etcd/common"

	"github.com/gin-gonic/gin"
)

func getTimeDuration(c *gin.Context, key string) (*time.Duration, error) {
	value, exist, err := getKey(c, key)
	if err != nil {
		return nil, err
	}

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
	value, exist, err := getKey(c, key)
	if err != nil {
		return nil, err
	}

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
	value, exist, err := getKey(c, key)
	if err != nil {
		return nil, nil
	}
	if exist {
		num, err := strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
		return &num, nil
	}
	return nil, nil
}

func getString(c *gin.Context, key string) (*string, error) {
	value, exist, err := getKey(c, key)
	if err != nil {
		return nil, nil
	}

	if exist {
		return gc.GetPointer(value), nil
	}
	return nil, nil
}

func getBool(c *gin.Context, key string) (*bool, error) {
	value, exist, err := getKey(c, key)
	if err != nil {
		return nil, nil
	}

	if exist {
		boolean, err := strconv.ParseBool(value)
		if err != nil {
			return nil, err
		}
		return &boolean, nil
	}
	return nil, nil
}

func getKey(c *gin.Context, key string) (value string, exist bool, err error) {
	method := c.Request.Method
	switch method {
	case http.MethodGet, http.MethodDelete:
		value, exist = c.GetQuery(key)
		return value, exist, nil
	case http.MethodPut:
		contentType := c.ContentType()
		if strings.Contains(contentType, "multipart/form-data") {
			// this is to support upload file with form-data on postman
			fileHeader, err := c.FormFile(key)
			if err == nil {
				file, err := fileHeader.Open()
				if err != nil {
					return "", false, err
				}
				fileContent, err := io.ReadAll(file)
				if err != nil {
					return "", false, err
				}
				return string(fileContent), true, nil
			} else {
				// not file
				value, exist = c.GetPostForm(key)
				return value, exist, nil
			}
		}
		if strings.Contains(contentType, "application/x-www-form-urlencoded") {
			// can use --data-urlencode on curl to upload file
			value, exist = c.GetPostForm(key)
			return value, exist, nil
		}
	default:
		return "", false, errors.New("getKey: unable to get value")
	}
	return "", false, nil
}

func parseDeleteRequest(c *gin.Context) (EtcdCommandRequest, error) {
	key := strings.TrimPrefix(c.Param("key"), "/")
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

	cmd.PrevValue, err = getString(c, "prevValue")
	if err != nil {
		return EtcdCommandRequest{}, common.CreateError("prevValue", err)
	}

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
	key := strings.TrimPrefix(c.Param("key"), "/")
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
	key := strings.TrimPrefix(c.Param("key"), "/")
	if key == "" {
		return EtcdCommandRequest{}, common.CreateError("key", errors.New("can't be empty"))
	}

	cmd := EtcdCommandRequest{
		Action: strings.ToLower(c.Request.Method),
		Key:    key,
	}

	var err error

	cmd.Value, err = getString(c, "value")
	if err != nil {
		return EtcdCommandRequest{}, common.CreateError("value", err)
	}

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

	cmd.PrevValue, err = getString(c, "prevValue")
	if err != nil {
		return EtcdCommandRequest{}, common.CreateError("prevValue", err)
	}

	cmd.PrevIndex, err = getInt(c, "prevIndex")
	if err != nil {
		return EtcdCommandRequest{}, common.CreateError("prevIndex", err)
	}

	return cmd, nil
}
