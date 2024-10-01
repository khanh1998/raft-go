package logic

import (
	"context"
	"errors"
	"khanh/raft-go/common"
	"time"

	"go.opentelemetry.io/otel/codes"
)

func (r *RaftBrainImpl) getLeaderHttpUrl() string {
	leaderUrl := ""
	for _, peer := range r.members {
		if peer.ID != r.id && peer.ID == r.leaderID {
			leaderUrl = peer.HttpUrl

			break
		}
	}

	return leaderUrl
}

func (r *RaftBrainImpl) ClientRequest(ctx context.Context, input *common.ClientRequestInput, output *common.ClientRequestOutput) (err error) {
	ctx, span := tracer.Start(ctx, "ClientRequest")
	defer span.End()

	defer func() {
		if output.Status == common.StatusNotOK {
			span.SetStatus(codes.Error, output.Response.(string))
		} else {
			span.SetStatus(codes.Ok, "finished client request")
		}
	}()

	r.inOutLock.Lock()
	if r.state != common.StateLeader {
		leaderUrl := r.getLeaderHttpUrl()

		*output = common.ClientRequestOutput{
			Status:     common.StatusNotOK,
			Response:   common.NotLeader,
			LeaderHint: leaderUrl,
		}

		r.inOutLock.Unlock()

		return nil
	}

	index := r.appendLog(ctx, common.Log{
		Term:        r.currentTerm,
		Command:     input.Command,
		ClientID:    input.ClientID,
		SequenceNum: input.SequenceNum,
	})

	r.inOutLock.Unlock()

	span.AddEvent("log appended")

	var status common.ClientRequestStatus = common.StatusOK
	var response any = nil

	if err := r.arm.Register(index); err != nil {
		r.log().ErrorContext(ctx, "ClientRequest_Register", err)

		*output = common.ClientRequestOutput{
			Status:     common.StatusNotOK,
			Response:   "can't register for async response" + err.Error(),
			LeaderHint: "",
		}

		return nil
	}

	response, err = r.arm.TakeResponse(index, 30*time.Second)
	if err != nil {
		status = common.StatusNotOK

		if errors.Is(err, common.ErrorSessionExpired) {
			response = common.SessionExpired
		}

		r.log().ErrorContext(ctx, "ClientRequest_TakeResponse", err)
	}

	span.AddEvent("log committed")

	*output = common.ClientRequestOutput{
		Status:   status,
		Response: response,
	}

	return nil
}

func (r *RaftBrainImpl) RegisterClient(ctx context.Context, input *common.RegisterClientInput, output *common.RegisterClientOutput) (err error) {
	ctx, span := tracer.Start(ctx, "RegisterClient")
	defer span.End()

	defer func() {
		if output.Status == common.StatusNotOK {
			span.SetStatus(codes.Error, output.Response.(string))
		} else {
			span.SetStatus(codes.Ok, "finished client request")
		}
	}()

	r.inOutLock.Lock()

	if r.state != common.StateLeader {
		leaderUrl := r.getLeaderHttpUrl()

		*output = common.RegisterClientOutput{
			Status:     common.StatusNotOK,
			LeaderHint: leaderUrl,
			Response:   common.NotLeader,
		}

		r.inOutLock.Unlock()

		return nil
	}

	index := r.appendLog(ctx, common.Log{
		Term:        r.currentTerm,
		ClientID:    0,
		SequenceNum: 0,
		Command:     "register",
	})

	r.inOutLock.Unlock()

	var status common.ClientRequestStatus = common.StatusOK

	if err := r.arm.Register(index); err != nil {
		r.log().ErrorContext(ctx, "RegisterClient_Register", err)
		*output = common.RegisterClientOutput{
			Status:     common.StatusNotOK,
			LeaderHint: "",
			Response:   "error: can't register for async response. " + err.Error(),
		}

		return nil
	}

	_, err = r.arm.TakeResponse(index, 30*time.Second)
	if err != nil {
		r.log().ErrorContext(ctx, "RegisterClient_TakeResponse", err)

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

func (r *RaftBrainImpl) ClientQuery(ctx context.Context, input *common.ClientQueryInput, output *common.ClientQueryOutput) (err error) {
	ctx, span := tracer.Start(ctx, "ClientQuery")
	defer span.End()

	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, output.Response.(string))
		} else {
			span.SetStatus(codes.Ok, "query success")
		}
	}()

	if r.state != common.StateLeader {
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
		log, err := r.GetLog(r.commitIndex)
		if err != nil {
			break
		}

		if log.Term == r.currentTerm {
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

	realIndex := r.commitIndex

	r.BroadcastAppendEntries(ctx) // TODO: heartbeat only

	ok = false
	for i := 0; i < 100; i++ {
		if r.commitIndex >= realIndex {
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

	res, err := r.stateMachine.Process(0, 0, input.Query, 0)
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
