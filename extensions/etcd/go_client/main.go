package go_client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/etcd/common"
	"khanh/raft-go/observability"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/exp/rand"
)

const (
	KeyApiPath = "/v2/keys/"
)

type HttpClient struct {
	client      http.Client
	nodeUrls    []gc.ClusterMember
	nodeUrlsMap map[int]gc.ClusterMember
	leaderId    int
	logger      observability.Logger
}

func NewHttpClient(nodeUrls []gc.ClusterMember, logger observability.Logger) HttpClient {
	if len(nodeUrls) == 0 {
		logger.Fatal("NewHttpClient: empty node url list", "nodeUrls", nodeUrls)
	}
	nodeUrlsMap := map[int]gc.ClusterMember{}
	for _, n := range nodeUrls {
		nodeUrlsMap[n.ID] = n
	}
	h := &HttpClient{
		client: http.Client{
			Timeout: 0, // timeout is unlimited, we need it for waiting for change on key
		},
		nodeUrls:    nodeUrls,
		nodeUrlsMap: nodeUrlsMap,
		leaderId:    0, // no leader was known
		logger:      logger,
	}

	h.client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		locationHeader := req.Header.Get("Location")
		if locationHeader != "" {
			// Check if the URL is missing the scheme (http:// or https://)
			parsedURL, err := url.Parse(locationHeader)
			if err == nil && parsedURL.Scheme == "" {
				// If the scheme is missing, prepend the base URL (assuming http://localhost:8080 as base)
				parsedURL, _ = url.Parse("http://" + locationHeader)
				req.Header.Set("Location", parsedURL.String())
			}

			if err != nil {
				logger.Error("url_parse", err, "Location", locationHeader)
			}

			logger.Info(
				"Redirecting to: ",
				"url", locationHeader,
				"fix_url", req.Header.Get("Location"),
				"nodeUrls", h.nodeUrls,
			)

			for _, node := range h.nodeUrls {
				if strings.Contains(locationHeader, node.HttpUrl) {
					h.leaderId = node.ID
				}
			}
		}
		return nil
	}

	return *h
}

var (
	ErrNodeIdDoesNotExist = errors.New("node's id doesn't exist")
	ErrNodeListIsEmpty    = errors.New("node list is empty")
	ErrNoLeaderCanBeFound = errors.New("no leader can be found")
)

func (h HttpClient) getUrl(id int) (string, error) {
	if node, ok := h.nodeUrlsMap[id]; ok {
		return node.HttpUrl, nil
	}
	return "", ErrNodeIdDoesNotExist
}

func (h HttpClient) leaderUrl() string {
	if h.leaderId != 0 {
		if node, ok := h.nodeUrlsMap[h.leaderId]; ok {
			return node.HttpUrl
		}
	}
	return ""
}

type GetRequest struct {
	Key       string
	Wait      bool
	WaitIndex int
	Prefix    bool
}

func (gr GetRequest) ToQueryString() string {
	values := url.Values{}
	if gr.Wait {
		values.Set("wait", strconv.FormatBool(gr.Wait))
	}
	if gr.WaitIndex > 0 {
		values.Set("waitIndex", strconv.Itoa(gr.WaitIndex))
	}
	if gr.Prefix {
		values.Set("prefix", strconv.FormatBool(gr.Prefix))
	}
	return values.Encode()
}

func handleResponse(httpRes *http.Response) (success common.EtcdResultRes, err error) {
	if httpRes.StatusCode >= 0 && httpRes.StatusCode < 300 {
		err = json.NewDecoder(httpRes.Body).Decode(&success)
		if err != nil {
			return success, err
		}
	} else {
		err = common.EtcdResultErr{}
		err = json.NewDecoder(httpRes.Body).Decode(&err)
		if err != nil {
			return success, err
		}
	}
	return success, err
}

// find an reachable server and send request.
// if we reach an follower it will redirect us to leader.
func (h HttpClient) findAndDo(ctx context.Context, httpReq *http.Request) (httpRes *http.Response, err error) {
	if len(h.nodeUrls) == 0 {
		return httpRes, ErrNodeListIsEmpty
	}
	tried := map[int]struct{}{}
	targetNodeIndex := 0
	if h.leaderId > 0 {
		for index, node := range h.nodeUrls {
			if node.ID == h.leaderId {
				targetNodeIndex = index
			}
		}
	}

	for i := 0; i < len(h.nodeUrls); i++ {
		if targetNodeIndex == 0 {
			// find a random server that we've never tried and send request to get leader hint
			for j := 0; j < 10; j++ {
				rand.Seed(uint64(time.Now().UnixNano()))
				targetNodeIndex = rand.Intn(len(h.nodeUrls))

				_, used := tried[targetNodeIndex]
				if !used {
					break
				}
			}
		}

		tried[targetNodeIndex] = struct{}{}

		httpReq.URL.Host = h.nodeUrls[targetNodeIndex].HttpUrl

		dumpReq, _ := httputil.DumpRequestOut(httpReq, true)
		h.logger.InfoContext(ctx, "findAndDo", "action", httpReq.Method, "dumpReq", string(dumpReq))

		httpRes, err = h.client.Do(httpReq)
		if err != nil {
			h.logger.ErrorContext(ctx, "findAndDo_do", err)
			targetNodeIndex = 0
			continue
		} else {
			dumpRes, _ := httputil.DumpResponse(httpRes, true)
			h.logger.InfoContext(ctx, "findAndDo", "action", httpReq.Method, "dumpRes", dumpRes)

			return httpRes, nil
		}

	}

	return httpRes, ErrNoLeaderCanBeFound
}

// find leader and send get request to it
func (h HttpClient) Get(ctx context.Context, req GetRequest) (success common.EtcdResultRes, err error) {
	leaderUrl := "http://" + h.leaderUrl() + KeyApiPath + req.Key
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, leaderUrl, nil)
	if err != nil {
		return success, err
	}
	httpReq.URL.RawQuery = req.ToQueryString()

	httpRes, err := h.findAndDo(ctx, httpReq)
	if err != nil {
		return success, err
	}
	defer httpRes.Body.Close()

	return handleResponse(httpRes)
}

type SetRequest struct {
	Key       string
	Value     *string
	TTL       string // empty, 0, positive (5s)
	PrevExist *bool
	PrevIndex int
	PrevValue *string
	Refresh   bool
}

func (sr SetRequest) ToFormData() string {
	values := url.Values{}
	if sr.Value != nil {
		values.Set("value", *sr.Value)
	}
	if sr.TTL != "" {
		_, err := time.ParseDuration(sr.TTL)
		if err != nil {
			panic(err.Error())
		}
		values.Set("ttl", sr.TTL)
	}
	if sr.PrevExist != nil {
		values.Set("prevExist", strconv.FormatBool(*sr.PrevExist))
	}
	if sr.PrevIndex > 0 {
		values.Set("prevIndex", strconv.Itoa(sr.PrevIndex))
	}
	if sr.PrevValue != nil {
		values.Set("prevValue", *sr.PrevValue)
	}
	if sr.Refresh {
		values.Set("refresh", strconv.FormatBool(sr.Refresh))
	}
	return values.Encode()
}

func (h HttpClient) Set(ctx context.Context, req SetRequest) (success common.EtcdResultRes, err error) {
	url := "http://" + h.leaderUrl() + KeyApiPath + req.Key
	formData := req.ToFormData()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewBufferString(formData))
	if err != nil {
		return success, err
	}
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	httpRes, err := h.findAndDo(ctx, httpReq)
	if err != nil {
		return success, err
	}
	defer httpRes.Body.Close()

	return handleResponse(httpRes)
}

type DeleteRequest struct {
	Key       string
	Prefix    bool
	PrevExist *bool
	PrevIndex int
	PrevValue *string
}

func (sr DeleteRequest) ToQueryString() string {
	values := url.Values{}
	if sr.Prefix {
		values.Set("prefix", strconv.FormatBool(sr.Prefix))
	}
	if sr.PrevExist != nil {
		values.Set("prevExist", strconv.FormatBool(*sr.PrevExist))
	}
	if sr.PrevIndex > 0 {
		values.Set("prevIndex", strconv.Itoa(sr.PrevIndex))
	}
	if sr.PrevValue != nil {
		values.Set("prevValue", *sr.PrevValue)
	}
	return values.Encode()
}

func (h HttpClient) Delete(ctx context.Context, req DeleteRequest) (success common.EtcdResultRes, err error) {
	url := "http://" + h.leaderUrl() + KeyApiPath + req.Key
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return success, err
	}
	httpReq.URL.RawQuery = req.ToQueryString()

	httpRes, err := h.findAndDo(ctx, httpReq)
	if err != nil {
		return success, err
	}
	defer httpRes.Body.Close()

	return handleResponse(httpRes)
}

func (h HttpClient) GetInfo(ctx context.Context, id int) (res gc.GetStatusResponse, err error) {
	url, err := h.getUrl(id)
	if err != nil {
		return res, err
	}

	url = "http://" + url + "/v2/info"

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return res, err
	}

	httpRes, err := h.client.Do(httpReq)
	if err != nil {
		return res, err
	}
	defer httpRes.Body.Close()

	dumpRes, _ := httputil.DumpResponse(httpRes, true)
	h.logger.InfoContext(ctx, "GetInfo", "url", url, "dump", dumpRes)

	err = json.NewDecoder(httpRes.Body).Decode(&res)
	if err != nil {
		return res, err
	}

	return res, nil
}
