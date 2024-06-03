package logic

import (
	"errors"
	"khanh/raft-go/common"
	"time"
)

func (r *RaftBrainImpl) getLeaderHttpUrl() string {
	leaderUrl := ""
	for _, peer := range r.members {
		if peer.ID != r.ID && peer.ID == r.LeaderID {
			leaderUrl = peer.HttpUrl

			break
		}
	}

	return leaderUrl
}

func (r *RaftBrainImpl) ClientRequest(input *common.ClientRequestInput, output *common.ClientRequestOutput) (err error) {
	r.inOutLock.Lock()
	if r.State != common.StateLeader {
		leaderUrl := r.getLeaderHttpUrl()

		*output = common.ClientRequestOutput{
			Status:     common.StatusNotOK,
			Response:   common.NotLeader,
			LeaderHint: leaderUrl,
		}

		r.inOutLock.Unlock()

		return nil
	}

	index := r.appendLog(common.Log{
		Term:        r.CurrentTerm,
		Command:     input.Command,
		ClientID:    input.ClientID,
		SequenceNum: input.SequenceNum,
	})

	r.inOutLock.Unlock()

	var status common.ClientRequestStatus = common.StatusOK
	var response any = nil

	if err := r.ARM.Register(index); err != nil {
		r.log().Err(err).Msg("ClientRequest_Register")

		*output = common.ClientRequestOutput{
			Status:     common.StatusNotOK,
			Response:   "can't register for async response" + err.Error(),
			LeaderHint: "",
		}

		return nil
	}

	response, err = r.ARM.TakeResponse(index, 30*time.Second)
	if err != nil {
		status = common.StatusNotOK

		if errors.Is(err, common.ErrorSessionExpired) {
			response = common.SessionExpired
		}

		r.log().Err(err).Msg("ClientRequest_TakeResponse")
	}

	*output = common.ClientRequestOutput{
		Status:   status,
		Response: response,
	}

	return nil
}

func (r *RaftBrainImpl) RegisterClient(input *common.RegisterClientInput, output *common.RegisterClientOutput) (err error) {
	r.inOutLock.Lock()

	if r.State != common.StateLeader {
		leaderUrl := r.getLeaderHttpUrl()

		*output = common.RegisterClientOutput{
			Status:     common.StatusNotOK,
			LeaderHint: leaderUrl,
			Response:   "error: connect to follower",
		}

		r.inOutLock.Unlock()

		return nil
	}

	index := r.appendLog(common.Log{
		Term:        r.CurrentTerm,
		ClientID:    0,
		SequenceNum: 0,
		Command:     "register",
	})

	r.inOutLock.Unlock()

	var status common.ClientRequestStatus = common.StatusOK

	if err := r.ARM.Register(index); err != nil {
		r.log().Err(err).Msg("RegisterClient_Register")
		*output = common.RegisterClientOutput{
			Status:     common.StatusNotOK,
			LeaderHint: "",
			Response:   "error: can't register for async response. " + err.Error(),
		}

		return nil
	}

	_, err = r.ARM.TakeResponse(index, 30*time.Second)
	if err != nil {
		r.log().Err(err).Msg("RegisterClient_TakeResponse")

		*output = common.RegisterClientOutput{
			Status:     common.StatusNotOK,
			LeaderHint: "",
			Response:   "timeout: wait for async response. " + err.Error(),
		}

		return nil
	}

	*output = common.RegisterClientOutput{
		Status:     status,
		Response:   index,
		LeaderHint: "",
	}

	return nil
}

func (r *RaftBrainImpl) ClientQuery(input *common.ClientQueryInput, output *common.ClientQueryOutput) (err error) {
	defer func() {
		if err != nil {
			output.Status = common.StatusNotOK
			output.Response = err.Error()
		}
	}()

	if r.State != common.StateLeader {
		leaderUrl := r.getLeaderHttpUrl()

		*output = common.ClientQueryOutput{
			Status:     common.StatusNotOK,
			Response:   common.NotLeader,
			LeaderHint: leaderUrl,
		}

		return nil
	}
	var ok bool
	for i := 0; i < 100; i++ {
		log, err := r.GetLog(r.CommitIndex)
		if err != nil {
			break
		}

		if log.Term == r.CurrentTerm {
			ok = true
			break
		}

		time.Sleep(time.Second)
	}

	if !ok {
		*output = common.ClientQueryOutput{
			Status:     common.StatusNotOK,
			Response:   "timeout: leader of current term haven't commit any log yet. " + err.Error(),
			LeaderHint: "",
		}

		return nil
	}

	realIndex := r.CommitIndex

	r.BroadcastAppendEntries() // TODO: heartbeat only

	ok = false
	for i := 0; i < 100; i++ {
		if r.CommitIndex >= realIndex {
			ok = true
			break
		}

		time.Sleep(time.Second)
	}

	if !ok {
		*output = common.ClientQueryOutput{
			Status:     common.StatusNotOK,
			Response:   "timeout: wait for log commiting",
			LeaderHint: "",
		}

		return nil
	}

	res, err := r.StateMachine.Process(0, 0, input.Query, 0)
	if err != nil {
		*output = common.ClientQueryOutput{
			Status:     common.StatusNotOK,
			Response:   err.Error(),
			LeaderHint: "",
		}

		return nil
	}

	*output = common.ClientQueryOutput{
		Status:     common.StatusOK,
		Response:   res,
		LeaderHint: "",
	}

	return nil
}
