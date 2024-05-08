package logic

import (
	"errors"
	"khanh/raft-go/common"
	"time"
)

func (r *RaftBrainImpl) getLeaderHttpUrl() string {
	leaderUrl := ""
	for _, peer := range r.Peers {
		if peer.ID != r.ID && peer.ID == r.VotedFor {
			leaderUrl = peer.HttpUrl

			break
		}
	}

	return leaderUrl
}

func (r *RaftBrainImpl) ClientRequest(input *common.ClientRequestInput, output *common.ClientRequestOutput) (err error) {
	r.InOutLock.Lock()
	if r.State != common.StateLeader {
		leaderUrl := r.getLeaderHttpUrl()

		*output = common.ClientRequestOutput{
			Status:     common.StatusNotOK,
			Response:   common.NotLeader,
			LeaderHint: leaderUrl,
		}

		r.InOutLock.Unlock()

		return nil
	}

	index := r.appendLog(common.Log{
		Term:        r.CurrentTerm,
		Command:     input.Command,
		ClientID:    input.ClientID,
		SequenceNum: input.SequenceNum,
	})

	r.InOutLock.Unlock()

	var status common.ClientRequestStatus = common.StatusOK
	var response any = nil

	if err := r.ARM.Register(index); err != nil {
		r.log().Err(err).Msg("ClientRequest_Register")
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
	r.InOutLock.Lock()

	if r.State != common.StateLeader {
		leaderUrl := r.getLeaderHttpUrl()

		*output = common.RegisterClientOutput{
			Status:     common.StatusNotOK,
			LeaderHint: leaderUrl,
		}

		r.InOutLock.Unlock()

		return nil
	}

	index := r.appendLog(common.Log{
		Term:        r.CurrentTerm,
		ClientID:    0,
		SequenceNum: 0,
		Command:     "register",
	})

	r.InOutLock.Unlock()

	var status common.ClientRequestStatus = common.StatusOK

	if err := r.ARM.Register(index); err != nil {
		r.log().Err(err).Msg("RegisterClient_Register")
	}

	_, err = r.ARM.TakeResponse(index, 30*time.Second)
	if err != nil {
		r.log().Err(err).Msg("RegisterClient_TakeResponse")

		status = common.StatusNotOK
	}

	*output = common.RegisterClientOutput{
		Status:     status,
		ClientID:   index,
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
		log, err := r.getLog(r.CommitIndex)
		if err != nil {
			return err
		}

		if log.Term == r.CurrentTerm {
			ok = true
			break
		}

		time.Sleep(time.Second)
	}

	if !ok {
		return errors.New("client query: Timeout 1")
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
		return errors.New("client query: Timeout 2")
	}

	res, err := r.StateMachine.Process(0, 0, input.Query, 0)
	if err != nil {
		return err
	}

	*output = common.ClientQueryOutput{
		Status:     common.StatusOK,
		Response:   res,
		LeaderHint: "",
	}

	return nil
}
