package integration_testing

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"khanh/raft-go/common"
	"khanh/raft-go/observability"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

type Request struct {
	ClientId    int    `json:"client_id"`
	SequenceNum int    `json:"sequence_num"`
	Command     string `json:"command"`
}

type Response struct {
	Status   common.ClientRequestStatus `json:"status"`
	Hint     string                     `json:"leader_hint"`
	Response any                        `json:"response"`
}

type HttpServerConnectionInfo struct {
	Id  int
	Url string
}

type HttpAgentArgs struct {
	serverUrls []HttpServerConnectionInfo
	Log        observability.Logger
}

type HttpAgent struct {
	serverUrls  map[int]string
	serverInfos []HttpServerConnectionInfo
	client      *http.Client
	leaderId    int
	clientId    int
	sequenceNum int
	log         observability.Logger
}

func NewHttpAgent(args HttpAgentArgs) *HttpAgent {
	serverUrls := map[int]string{}
	for _, s := range args.serverUrls {
		serverUrls[s.Id] = s.Url
	}

	return &HttpAgent{
		client: &http.Client{
			Timeout: 60 * time.Second,
		},
		serverUrls:  serverUrls,
		leaderId:    0, // leader is unknown
		clientId:    0,
		sequenceNum: 0,
		serverInfos: args.serverUrls,
		log:         args.Log,
	}
}

func (h *HttpAgent) RemoveServer(id int, httpUrl, rpcUrl string) (err error) {
	h.sequenceNum += 1
	payload := Request{
		Command:     fmt.Sprintf("removeServer %d %s %s", id, httpUrl, rpcUrl),
		ClientId:    h.clientId,
		SequenceNum: h.sequenceNum,
	}

	result, err := h.findLeaderAndDo(payload)
	if err != nil {
		return err
	}

	if result.Status == common.StatusNotOK {
		return fmt.Errorf("response is not ok: %v", result.Response)
	}

	return nil
}

func (h *HttpAgent) AddServer(id int, httpUrl, rpcUrl string) (err error) {
	h.sequenceNum += 1
	payload := Request{
		Command:     fmt.Sprintf("addServer %d %s %s", id, httpUrl, rpcUrl),
		ClientId:    h.clientId,
		SequenceNum: h.sequenceNum,
	}

	result, err := h.findLeaderAndDo(payload)
	if err != nil {
		return err
	}

	if result.Status == common.StatusNotOK {
		return fmt.Errorf("response is not ok: %v", result.Response)
	}

	return nil
}

func (h *HttpAgent) ClientRequest(key string, value string) (err error) {
	h.sequenceNum += 1
	payload := Request{
		Command:     fmt.Sprintf("set %s %s", key, value),
		ClientId:    h.clientId,
		SequenceNum: h.sequenceNum,
	}

	result, err := h.findLeaderAndDo(payload)
	if err != nil {
		return err
	}

	if result.Status == "" || result.Status == common.StatusNotOK {
		return fmt.Errorf("response is not ok: %v", result.Response)
	}

	return nil
}

func (h *HttpAgent) ClientQuery(key string) (value string, err error) {
	payload := Request{
		Command:     fmt.Sprintf("get %s", key),
		ClientId:    h.clientId,
		SequenceNum: h.sequenceNum,
	}

	result, err := h.findLeaderAndDo(payload)
	if err != nil {
		return "", err
	}

	log.Debug().Interface("result", result).Msg("findLeaderAndDo")

	if result.Status == common.StatusNotOK {
		return "", fmt.Errorf("response is not ok: %v", result.Response)
	}

	value, ok := result.Response.(string)
	if !ok {
		return "", fmt.Errorf("can't cast response value [%v] to string", result.Response)
	}

	return value, nil
}

func (h *HttpAgent) RegisterClient() (err error) {
	payload := Request{
		Command: "register",
	}

	result, err := h.findLeaderAndDo(payload)
	if err != nil {
		return err
	}

	log.Debug().Interface("result", result).Msg("findLeaderAndDo")

	if result.Status == common.StatusNotOK {
		return fmt.Errorf("response is not ok: %v", result.Response)
	}

	h.clientId = int(result.Response.(float64))
	return nil
}

func (h *HttpAgent) findLeaderAndDo(payload Request) (result Response, err error) {
	tried := map[int]struct{}{0: {}}

	for i := 0; i < len(h.serverInfos); i++ {
		if h.leaderId == 0 {
			// find a random server that we've never tried and send request to get leader hint
			randIndex, leaderId := -1, 0
			min, max := int64(0), int64(len(h.serverInfos))
			for {
				randIndex = int(common.RandInt(min, max))
				leaderId = h.serverInfos[randIndex].Id

				_, used := tried[leaderId]
				if !used {
					break
				}
			}
			h.leaderId = leaderId
		}

		tried[h.leaderId] = struct{}{}

		url := "http://" + h.serverUrls[h.leaderId] + "/cli"
		// try with current leader id (id can be outdated)
		result, err = h.do(url, payload)
		if err != nil {
			h.log.Error("findLeaderAndDo_do", err)
			h.leaderId = 0
			continue
		}

		// hit leader
		if result.Status == common.StatusOK || result.Response != common.NotLeader {
			return result, nil
		}

		// hit follower
		h.leaderId = 0

		// follwer server can't provide hint, try other server
		if result.Hint == "" {
			continue
		}

		// find leader id from the hint
		for id, serverUrl := range h.serverUrls {
			if result.Hint == serverUrl {
				h.leaderId = id
				break
			}
		}
	}

	return result, errors.New("no leader can be found")
}

func (h HttpAgent) do(url string, payload Request) (result Response, err error) {
	method := http.MethodPost
	var buf bytes.Buffer
	err = json.NewEncoder(&buf).Encode(payload)
	if err != nil {
		return result, err
	}

	req, err := http.NewRequest(method, url, &buf)
	if err != nil {
		return result, err
	}

	req.Header.Add("Content-Type", "application/json")

	res, err := h.client.Do(req)
	if err != nil {
		return result, err
	}
	defer res.Body.Close()

	// if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
	// 	return
	// }
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return result, err
	}

	responseBody := string(bodyBytes)

	if err := json.NewDecoder(strings.NewReader(responseBody)).Decode(&result); err != nil {
		return result, err
	}

	h.log.Debug("ResponseToString", "response", responseBody, "decoded", result)

	return result, nil
}
