package logic

import (
	"context"
	"errors"
	"fmt"
	"khanh/raft-go/common"
	"strconv"
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

func (r *RaftBrainImpl) KeepAlive(ctx context.Context, input *common.KeepAliveClientInput, output *common.KeepAliveClientOutput) (err error) {
	ctx, span := tracer.Start(ctx, "ClientRequest")
	defer span.End()
	defer func() {
		if output.Status == common.StatusNotOK {
			span.SetStatus(codes.Error, output.Response)
		} else {
			span.SetStatus(codes.Ok, "finished client request")
		}
	}()

	r.inOutLock.Lock()
	if r.state != common.StateLeader {
		leaderUrl := r.getLeaderHttpUrl()

		*output = common.KeepAliveClientOutput{
			Status:     common.StatusNotOK,
			Response:   common.NotLeader,
			LeaderHint: leaderUrl,
		}

		r.inOutLock.Unlock()

		return nil
	}

	newLog := common.Log{
		Term:        r.persistState.GetCurrentTerm(),
		Command:     "keep-alive",
		ClientID:    input.ClientID,
		SequenceNum: input.SequenceNum,
		ClusterTime: r.clusterClock.Interpolate(),
	}

	index, err := r.appendLog(ctx, newLog)
	if err != nil {
		r.log().ErrorContext(ctx, "KeepAlive_appendLog", err)

		*output = common.KeepAliveClientOutput{
			Status:     common.StatusNotOK,
			Response:   "append log err: " + err.Error(),
			LeaderHint: "",
		}
	}

	r.inOutLock.Unlock()

	span.AddEvent("log appended")

	var status common.ClientRequestStatus = common.StatusOK
	var response string = ""

	if err := r.arm.Register(index); err != nil {
		r.log().ErrorContext(ctx, "KeepAlive_Register", err)

		*output = common.KeepAliveClientOutput{
			Status:     common.StatusNotOK,
			Response:   "can't keep this session alive: " + err.Error(),
			LeaderHint: "",
		}

		return nil
	}

	response, err = r.arm.TakeResponse(index, 30*time.Second)
	if err != nil {
		status = common.StatusNotOK

		response = fmt.Sprintf("time out: %s", err.Error())
		if errors.Is(err, common.ErrorSessionExpired) {
			response = common.SessionExpired
		}

		r.log().ErrorContext(ctx, "KeepAlive_TakeResponse", err)
	}

	span.AddEvent("log committed")

	*output = common.KeepAliveClientOutput{
		Status:   status,
		Response: response,
	}

	return nil
}

func (r *RaftBrainImpl) ClientRequest(ctx context.Context, input *common.ClientRequestInput, output *common.ClientRequestOutput) (err error) {
	ctx, span := tracer.Start(ctx, "ClientRequest")
	defer span.End()

	defer func() {
		if output.Status == common.StatusNotOK {
			span.SetStatus(codes.Error, output.Response)
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

	newLog := common.Log{
		Term:        r.persistState.GetCurrentTerm(),
		Command:     input.Command,
		ClientID:    input.ClientID,
		SequenceNum: input.SequenceNum,
		ClusterTime: r.clusterClock.Interpolate(),
	}

	index, err := r.appendLog(ctx, newLog)
	if err != nil {
		r.log().ErrorContext(ctx, "ClientRequest_appendLog", err)

		*output = common.ClientRequestOutput{
			Status:     common.StatusNotOK,
			Response:   "append log err: " + err.Error(),
			LeaderHint: "",
		}
	}

	r.inOutLock.Unlock()

	span.AddEvent("log appended")

	var status common.ClientRequestStatus = common.StatusOK
	var response string = ""

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

		response = fmt.Sprintf("timeout: %s", err.Error())
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
			span.SetStatus(codes.Error, output.Response)
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

	newLog := common.Log{
		Term:        r.persistState.GetCurrentTerm(),
		ClientID:    0,
		SequenceNum: 0,
		Command:     "register",
		ClusterTime: r.clusterClock.Interpolate(),
	}

	index, err := r.appendLog(ctx, newLog)
	if err != nil {
		r.log().ErrorContext(ctx, "RegisterClient_appendLog", err)

		*output = common.RegisterClientOutput{
			Status:     common.StatusNotOK,
			Response:   "append log err: " + err.Error(),
			LeaderHint: "",
		}
	}

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
		Response:   strconv.Itoa(index),
		LeaderHint: "",
	}

	return nil
}

func (r *RaftBrainImpl) ClientQuery(ctx context.Context, input *common.ClientQueryInput, output *common.ClientQueryOutput) (err error) {
	ctx, span := tracer.Start(ctx, "ClientQuery")
	defer span.End()

	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, output.Response)
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
	err = errors.New("timeout")
	for i := 0; i < 100; i++ {
		log, err := r.GetLog(r.commitIndex)
		if err != nil && !errors.Is(err, common.ErrLogIsInSnapshot) {
			break
		}

		if log.Term == r.persistState.GetCurrentTerm() {
			ok = true
			break
		}

		time.Sleep(r.heartBeatTimeOutMin)
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

		time.Sleep(r.heartBeatTimeOutMin)
	}

	if !ok {
		*output = common.ClientQueryOutput{
			Status:     common.StatusNotOK,
			Response:   "timeout: wait for log committing",
			LeaderHint: "",
		}

		return nil
	}

	res, err := r.stateMachine.Process(ctx, 0, common.Log{Command: input.Query, ClusterTime: r.clusterClock.clusterTimeAtEpoch})
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
