package go_client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	KeyApiPath    = "/v2/keys/"
	MemberApiPath = "/v2/members/"
)

type ResponseHeader struct {
	EtcdIndex int
	RaftIndex int
	RaftTerm  int
}

type HttpClient struct {
	client      http.Client
	nodeUrls    []gc.ClusterMember
	nodeUrlsMap map[int]gc.ClusterMember
	leaderId    int
	logger      observability.Logger
}

func NewHttpClient(nodeUrls []gc.ClusterMember, logger observability.Logger) *HttpClient {
	if len(nodeUrls) == 0 {
		logger.Fatal("NewHttpClient: empty node url list", "nodeUrls", nodeUrls)
	}
	h := &HttpClient{
		client: http.Client{
			Timeout: 0, // timeout is unlimited, we need it for waiting for change on key
		},
		nodeUrls:    []gc.ClusterMember{},
		nodeUrlsMap: map[int]gc.ClusterMember{},
		leaderId:    0, // no leader was known
		logger:      logger,
	}

	for _, node := range nodeUrls {
		h.addMemberInfo(node)
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

	return h
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

type EtcdResponse struct {
	Action    string            `json:"action"`
	Node      common.KeyValue   `json:"node,omitempty"`
	Nodes     []common.KeyValue `json:"nodes,omitempty"` // to get prefix
	PrevNode  common.KeyValue   `json:"prevNode,omitempty"`
	PrevNodes []common.KeyValue `json:"prevNodes,omitempty"`
	ResponseHeader
}

func (h *HttpClient) handleKeyApiResponse(ctx context.Context, httpRes *http.Response) (success EtcdResponse, err error) {
	value := httpRes.Header.Get("X-Etcd-Index")
	if value != "" {
		success.EtcdIndex, err = strconv.Atoi(value)
		if err != nil {
			h.logger.ErrorContext(ctx, "handleResponse", fmt.Errorf("invalid value: X-Etcd-Index, %w", err), "value", value)
		}
	}
	value = httpRes.Header.Get("X-Raft-Index")
	if value != "" {
		success.RaftIndex, err = strconv.Atoi(value)
		if err != nil {
			h.logger.ErrorContext(ctx, "handleResponse", fmt.Errorf("invalid value: X-Raft-Index, %w", err), "value", value)
		}
	}
	value = httpRes.Header.Get("X-Raft-Term")
	if value != "" {
		success.RaftTerm, err = strconv.Atoi(value)
		if err != nil {
			h.logger.ErrorContext(ctx, "handleResponse", fmt.Errorf("invalid value: X-Raft-Term, %w", err), "value", value)
		}
	}

	if httpRes.StatusCode >= 0 && httpRes.StatusCode < 300 {
		err = json.NewDecoder(httpRes.Body).Decode(&success)
		if err != nil {
			return success, err
		}
	} else {
		failedRes := common.EtcdResultErr{}
		err = json.NewDecoder(httpRes.Body).Decode(&failedRes)
		if err == nil {
			return success, failedRes
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

		httpRes, err = h.client.Do(httpReq)
		if err != nil {
			h.logger.ErrorContext(
				ctx, "findAndDo_do", err,
				"dumpReq", dumpReq,
			)
			targetNodeIndex = 0
			continue
		} else {
			dumpRes, _ := httputil.DumpResponse(httpRes, true)
			h.logger.InfoContext(
				ctx, "findAndDo", "action", httpReq.Method,
				"dumpReq", dumpReq,
				"dumpRes", dumpRes,
			)

			return httpRes, nil
		}

	}

	return httpRes, ErrNoLeaderCanBeFound
}

// find leader and send get request to it
func (h HttpClient) Get(ctx context.Context, req GetRequest) (success EtcdResponse, err error) {
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

	return h.handleKeyApiResponse(ctx, httpRes)
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

func (h HttpClient) Set(ctx context.Context, req SetRequest) (success EtcdResponse, err error) {
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

	return h.handleKeyApiResponse(ctx, httpRes)
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

func (h HttpClient) Delete(ctx context.Context, req DeleteRequest) (success EtcdResponse, err error) {
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

	return h.handleKeyApiResponse(ctx, httpRes)
}

func handleMemberApiResponse(httpRes *http.Response) (err error) {
	if httpRes.StatusCode >= 0 && httpRes.StatusCode < 300 {
		return nil
	} else {
		err = common.EtcdResultErr{}
		err = json.NewDecoder(httpRes.Body).Decode(&err)
		if err != nil {
			return err
		}
	}
	return nil
}

type ClusterMemberRequest struct {
	gc.ClusterMember
}

func (c ClusterMemberRequest) ToQueryString() string {
	values := url.Values{}
	values.Set("httpUrl", c.HttpUrl)
	values.Set("rpcUrl", c.RpcUrl)
	return values.Encode()
}

func (h HttpClient) GetMembers(ctx context.Context) (members []gc.ClusterMember, err error) {
	url := "http://" + h.leaderUrl() + KeyApiPath
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return members, err
	}

	httpRes, err := h.findAndDo(ctx, httpReq)
	if err != nil {
		return members, err
	}
	defer httpRes.Body.Close()

	err = json.NewDecoder(httpRes.Body).Decode(&members)
	if err != nil {
		return members, err
	}

	return members, nil
}

func (h *HttpClient) removeMemberInfo(mem gc.ClusterMember) {
	removeIndex := -1
	for index, node := range h.nodeUrls {
		if node.ID == mem.ID {
			removeIndex = index
			break
		}
	}
	h.nodeUrls = append(h.nodeUrls[:removeIndex], h.nodeUrls[removeIndex+1:]...)
	delete(h.nodeUrlsMap, mem.ID)
}

func (h *HttpClient) addMemberInfo(mem gc.ClusterMember) {
	h.nodeUrls = append(h.nodeUrls, mem)
	h.nodeUrlsMap[mem.ID] = mem
}

func (h *HttpClient) AddMember(ctx context.Context, req ClusterMemberRequest) (err error) {
	defer func() {
		if err == nil {
			h.addMemberInfo(req.ClusterMember)
		}
	}()

	url := fmt.Sprintf("http://%s%s%d", h.leaderUrl(), MemberApiPath, req.ID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	httpReq.URL.RawQuery = req.ToQueryString()

	httpRes, err := h.findAndDo(ctx, httpReq)
	if err != nil {
		return err
	}
	defer httpRes.Body.Close()

	return handleMemberApiResponse(httpRes)
}

func (h *HttpClient) RemoveMember(ctx context.Context, req ClusterMemberRequest) (err error) {
	defer func() {
		if err == nil {
			h.removeMemberInfo(req.ClusterMember)
		}
	}()

	url := fmt.Sprintf("http://%s%s%d", h.leaderUrl(), MemberApiPath, req.ID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	httpReq.URL.RawQuery = req.ToQueryString()

	httpRes, err := h.findAndDo(ctx, httpReq)
	if err != nil {
		return err
	}
	defer httpRes.Body.Close()

	return handleMemberApiResponse(httpRes)
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
