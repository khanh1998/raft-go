package go_client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	gc "khanh/raft-go/common"
	"khanh/raft-go/extensions/etcd/common"
	"khanh/raft-go/observability"
	"net"
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

type Member struct {
	ID     int
	Host   string // localhost:8080
	Scheme string // http or https
}

type HttpClient struct {
	client      http.Client
	nodeUrls    []Member
	nodeUrlsMap map[int]Member
	leaderId    int
	logger      observability.Logger
}

// NewHttpClient creates a new client for interacting with the cluster.
func NewHttpClient(nodeUrls []Member, logger observability.Logger) (*HttpClient, error) {
	if len(nodeUrls) == 0 {
		logger.Fatal("NewHttpClient: empty node url list", "nodeUrls", nodeUrls)
	}
	h := &HttpClient{
		client: http.Client{
			Timeout: 0, // timeout is unlimited, we need it for waiting for change on key
		},
		nodeUrls:    []Member{},
		nodeUrlsMap: map[int]Member{},
		leaderId:    0, // no leader was known
		logger:      logger,
	}

	for _, node := range nodeUrls {
		err := h.addMemberInfo(node.ID, node.Host, node.Scheme)
		if err != nil {
			return nil, err
		}
	}

	h.client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		// this function shouldn't return any error,
		// if this func returns error,
		// it will stop the process of finding a reachable node of the method `h.do`
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
				if strings.Contains(locationHeader, node.Host) {
					h.leaderId = node.ID
				}
			}
		}
		return nil
	}

	return h, nil
}

var (
	ErrNodeIdDoesNotExist = errors.New("node's id doesn't exist")
	ErrNodeListIsEmpty    = errors.New("node list is empty")
	ErrNoLeaderCanBeFound = errors.New("no leader can be found")
)

// getUrl returns url of an node, like http://localhost:8080
func (h HttpClient) getUrl(id int) (string, error) {
	if node, ok := h.nodeUrlsMap[id]; ok {
		return fmt.Sprintf("%s://%s", node.Scheme, node.Host), nil
	}
	return "", ErrNodeIdDoesNotExist
}

// leaderUrl returns url of the current leader, like http://localhost:8080
func (h HttpClient) leaderUrl() string {
	if h.leaderId != 0 {
		if node, ok := h.nodeUrlsMap[h.leaderId]; ok {
			return fmt.Sprintf("%s://%s", node.Scheme, node.Host)
		}
	}
	return ""
}

// default the client will find and send request to a random node,
// and follow the redirection to the current leader.
// this option is for testing purpose, when you want to send request to a specified node.
// it will find node's host by `NodeId`,
// if nodeId doesn't exist, it will use the `NodeHost`.
type SelectedNode struct {
	NodeId int

	NodeHost string // format http://localhost:8080
}

type GetRequest struct {
	Key       string
	Wait      bool
	WaitIndex int
	Prefix    bool

	Target *SelectedNode
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

// similar to `findAndDo` but it's not random
func (h HttpClient) findAndDo2(ctx context.Context, method string, path string, data *string, headers map[string]string) (httpRes *http.Response, err error) {
	if len(h.nodeUrls) == 0 {
		return httpRes, ErrNodeListIsEmpty
	}

	if h.leaderId > 0 {
		httpRes, err = h.doWithoutFind(ctx, method, path, data, headers, SelectedNode{NodeId: h.leaderId})
		if err == nil {
			return httpRes, nil
		}
	}

	for _, node := range h.nodeUrls {
		url := fmt.Sprintf("%s://%s%s", node.Scheme, node.Host, path)
		var body io.Reader
		if data != nil {
			body = bytes.NewBufferString(*data)
		}
		httpReq, err := http.NewRequestWithContext(ctx, method, url, body)
		if err != nil {
			return nil, err
		}
		for key, value := range headers {
			httpReq.Header.Add(key, value)
		}

		dumpReq, _ := httputil.DumpRequestOut(httpReq, true)

		httpRes, err = h.client.Do(httpReq)
		if err != nil {
			h.logger.ErrorContext(
				ctx, "findAndDo_do", err,
				"dumpReq", dumpReq,
			)
			continue
		} else {
			dumpRes, _ := httputil.DumpResponse(httpRes, true)
			h.logger.InfoContext(
				ctx, "findAndDo", "action", httpReq.Method,
				"dumpReq", dumpReq,
				"dumpRes", dumpRes,
			)

			// follower will return 503 if it doesn't know which one is current leader
			if httpRes.StatusCode == http.StatusServiceUnavailable {
				continue
			} else {
				return httpRes, nil
			}
		}

	}

	return httpRes, ErrNoLeaderCanBeFound
}

// findAndDo keeps sending request to nodes until found a response,
// since some nodes might be crashed and will not response.
// if we reach an follower, it can redirect us to leader.
func (h HttpClient) findAndDo(ctx context.Context, method string, path string, data *string, headers map[string]string) (httpRes *http.Response, err error) {
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

	for {
		if len(tried) >= len(h.nodeUrls) {
			break
		}

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

		target := h.nodeUrls[targetNodeIndex]
		url := fmt.Sprintf("%s://%s%s", target.Scheme, target.Host, path)
		var body io.Reader
		if data != nil {
			body = bytes.NewBufferString(*data)
		}
		httpReq, err := http.NewRequestWithContext(ctx, method, url, body)
		if err != nil {
			return nil, err
		}
		for key, value := range headers {
			httpReq.Header.Add(key, value)
		}

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

			// follower will return 503 if it doesn't know which one is current leader
			if httpRes.StatusCode == http.StatusServiceUnavailable {
				continue
			} else {
				return httpRes, nil
			}
		}

	}

	return httpRes, ErrNoLeaderCanBeFound
}

func (h HttpClient) doWithoutFind(ctx context.Context, method string, path string, data *string, headers map[string]string, selected SelectedNode) (httpRes *http.Response, err error) {
	h.logger.InfoContext(ctx, "doWithoutFind", "nodes", h.nodeUrlsMap, "selected", selected)
	node, ok := h.nodeUrlsMap[selected.NodeId]
	var body io.Reader
	if data != nil {
		body = bytes.NewBufferString(*data)
	}
	url := ""
	if !ok {
		scheme, host, port, err := ParseHttpUrl(selected.NodeHost)
		if err != nil {
			return nil, fmt.Errorf("selected node's address is wrong: %w", err)
		}
		url = fmt.Sprintf("%s://%s:%s%s", scheme, host, port, path)
	} else {
		url = fmt.Sprintf("%s://%s%s", node.Scheme, node.Host, path)
	}

	h.logger.Info("doWithoutFind", "url", url, "node", node)

	httpReq, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}

	for key, value := range headers {
		httpReq.Header.Add(key, value)
	}

	dumpReq, _ := httputil.DumpRequestOut(httpReq, true)
	httpRes, err = h.client.Do(httpReq)
	if err != nil {
		h.logger.ErrorContext(
			ctx, "doWithoutFind_do", err,
			"dumpReq", dumpReq,
		)
		return nil, err
	}

	dumpRes, _ := httputil.DumpResponse(httpRes, true)
	h.logger.InfoContext(
		ctx, "doWithoutFind", "action", httpReq.Method,
		"dumpReq", dumpReq,
		"dumpRes", dumpRes,
	)

	return httpRes, nil
}

func (h HttpClient) do(ctx context.Context, method string, path string, data *string, headers map[string]string, selected *SelectedNode) (httpRes *http.Response, err error) {
	if selected != nil {
		return h.doWithoutFind(ctx, method, path, data, headers, *selected)
	}
	return h.findAndDo2(ctx, method, path, data, headers)
}

// find leader and send get request to it
func (h HttpClient) Get(ctx context.Context, req GetRequest) (success EtcdResponse, err error) {
	path := KeyApiPath + req.Key + "?" + req.ToQueryString()

	httpRes, err := h.do(ctx, http.MethodGet, path, nil, nil, req.Target)
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

	// submit requests to server without waiting for response
	IgnoreResponse bool

	Target *SelectedNode
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
	path := KeyApiPath + req.Key
	formData := req.ToFormData()

	headers := map[string]string{"Content-Type": "application/x-www-form-urlencoded"}

	if req.IgnoreResponse {
		go func() {
			h.do(ctx, http.MethodPut, path, &formData, headers, req.Target)
		}()

		success.Action = "submit"

		return success, nil
	} else {
		httpRes, err := h.do(ctx, http.MethodPut, path, &formData, headers, req.Target)
		if err != nil {
			return success, err
		}
		defer httpRes.Body.Close()

		return h.handleKeyApiResponse(ctx, httpRes)
	}
}

type DeleteRequest struct {
	Key       string
	Prefix    bool
	PrevExist *bool
	PrevIndex int
	PrevValue *string

	Target *SelectedNode
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
	path := KeyApiPath + req.Key + "?" + req.ToQueryString()

	httpRes, err := h.do(ctx, http.MethodDelete, path, nil, nil, req.Target)
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
	Https bool

	// optional: select a node to process your request
	Target *SelectedNode
}

func (c ClusterMemberRequest) ToQueryString() (string, error) {
	host, port, err := net.SplitHostPort(c.RpcUrl)
	if err != nil {
		return "", err
	}
	rpcUrl := fmt.Sprintf("%s:%s", host, port)

	host, port, err = net.SplitHostPort(c.HttpUrl)
	if err != nil {
		return "", err
	}

	httpUrl := fmt.Sprintf("%s:%s", host, port)

	ParseRpcUrl(c.RpcUrl)
	values := url.Values{}
	values.Set("httpUrl", httpUrl)
	values.Set("rpcUrl", rpcUrl)
	return values.Encode(), nil
}

func (h HttpClient) GetMembers(ctx context.Context, target *SelectedNode) (members []gc.ClusterMember, err error) {
	path := KeyApiPath

	httpRes, err := h.do(ctx, http.MethodGet, path, nil, nil, target)
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

func (h *HttpClient) removeMemberInfo(id int) {
	removeIndex := -1
	for index, node := range h.nodeUrls {
		if node.ID == id {
			removeIndex = index
			break
		}
	}
	h.nodeUrls = append(h.nodeUrls[:removeIndex], h.nodeUrls[removeIndex+1:]...)
	delete(h.nodeUrlsMap, id)
}

// addMemberInfo adds node's http url to client
// The HttpUrl should follow the format "host:port", e.g., "localhost:8080".
// the scheme is http or https
func (h *HttpClient) addMemberInfo(id int, httpUrl string, scheme string) error {
	if _, ok := h.nodeUrlsMap[id]; ok {
		return fmt.Errorf("member id %d is already added", id)
	}

	host, port, err := net.SplitHostPort(httpUrl)
	if err != nil {
		return err
	}

	m := Member{ID: id, Host: host + ":" + port, Scheme: scheme}

	h.nodeUrls = append(h.nodeUrls, m)
	h.nodeUrlsMap[id] = m

	return nil
}

// AddMember send request to cluster's leader to join a new node
func (h *HttpClient) AddMember(ctx context.Context, req ClusterMemberRequest) (err error) {
	defer func() {
		if err == nil {
			id := req.ID
			if req.Https {
				h.addMemberInfo(id, req.HttpUrl, "https")
			} else {
				h.addMemberInfo(id, req.HttpUrl, "http")
			}
		}
	}()

	query, err := req.ToQueryString()
	if err != nil {
		return err
	}

	path := fmt.Sprintf("%s%d?%s", MemberApiPath, req.ID, query)
	httpRes, err := h.do(ctx, http.MethodPost, path, nil, nil, req.Target)
	if err != nil {
		return err
	}
	defer httpRes.Body.Close()

	return handleMemberApiResponse(httpRes)
}

func (h *HttpClient) RemoveMember(ctx context.Context, req ClusterMemberRequest) (err error) {
	defer func() {
		if err == nil {
			h.removeMemberInfo(req.ID)
		}
	}()

	query, err := req.ToQueryString()
	if err != nil {
		return err
	}

	path := fmt.Sprintf("%s%d?%s", MemberApiPath, req.ID, query)

	httpRes, err := h.do(ctx, http.MethodDelete, path, nil, nil, req.Target)
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

	url = url + "/v2/info"

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
