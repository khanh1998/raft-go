package logic

import (
	"context"
	"errors"
	"fmt"
	gc "khanh/raft-go/common"
	"khanh/raft-go/raft_core/common"
	"net/http"
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

func (r *RaftBrainImpl) KeepAlive(ctx context.Context, input gc.Log, output *gc.KeepAliveClientOutput) (err error) {
	ctx, span := tracer.Start(ctx, "ClientRequest")
	defer span.End()
	defer func() {
		if output.Status == gc.StatusNotOK {
			span.SetStatus(codes.Error, "not ok")
		} else {
			span.SetStatus(codes.Ok, "finished client request")
		}
	}()

	r.inOutLock.Lock()
	if r.state != gc.StateLeader {
		leaderUrl := r.getLeaderHttpUrl()

		*output = gc.KeepAliveClientOutput{
			Status:     gc.StatusNotOK,
			Response:   gc.NotLeader,
			LeaderHint: leaderUrl,
		}

		r.inOutLock.Unlock()

		return nil
	}

	newLog, err := r.logFactory.AttachTermAndTime(input, r.GetCurrentTerm(), r.clusterClock.LeaderStamp())
	if err != nil {
		*output = gc.KeepAliveClientOutput{
			Status:   gc.StatusNotOK,
			Response: err.Error(),
		}

		return nil
	}

	index, err := r.appendLog(ctx, newLog)
	if err != nil {
		r.log().ErrorContext(ctx, "KeepAlive_appendLog", err)

		*output = gc.KeepAliveClientOutput{
			Status:     gc.StatusNotOK,
			Response:   "append log err: " + err.Error(),
			LeaderHint: "",
		}
	}

	r.inOutLock.Unlock()

	span.AddEvent("log appended")

	var status gc.ClientRequestStatus = gc.StatusOK
	var response gc.LogResult

	if err := r.arm.Register(index); err != nil {
		r.log().ErrorContext(ctx, "KeepAlive_Register", err)

		*output = gc.KeepAliveClientOutput{
			Status:     gc.StatusNotOK,
			Response:   "can't keep this session alive: " + err.Error(),
			LeaderHint: "",
		}

		return nil
	}

	response, err = r.arm.TakeResponse(index, 30*time.Second)
	if err != nil {
		status = gc.StatusNotOK

		response = fmt.Sprintf("time out: %s", err.Error())
		if errors.Is(err, common.ErrorSessionExpired) {
			response = gc.SessionExpired
		}

		r.log().ErrorContext(ctx, "KeepAlive_TakeResponse", err)
	}

	span.AddEvent("log committed")

	*output = gc.KeepAliveClientOutput{
		Status:   status,
		Response: response,
	}

	return nil
}

func (r *RaftBrainImpl) ClientRequest(ctx context.Context, input gc.Log, output *gc.ClientRequestOutput) (err error) {
	ctx, span := tracer.Start(ctx, "ClientRequest")
	defer span.End()

	defer func() {
		if output.Status == gc.StatusNotOK {
			span.SetStatus(codes.Error, "not ok")
		} else {
			span.SetStatus(codes.Ok, "finished client request")
		}
	}()

	r.inOutLock.Lock()
	if r.state != gc.StateLeader {
		leaderUrl := r.getLeaderHttpUrl()

		*output = gc.ClientRequestOutput{
			Status:     gc.StatusNotOK,
			Response:   gc.NotLeader,
			LeaderHint: leaderUrl,
		}

		r.inOutLock.Unlock()

		return gc.RaftError{LeaderHint: leaderUrl, Message: gc.NotLeader, HttpCode: http.StatusMovedPermanently}
	}

	newLog, err := r.logFactory.AttachTermAndTime(input, r.GetCurrentTerm(), r.clusterClock.LeaderStamp())
	if err != nil {
		*output = gc.ClientRequestOutput{
			Status:   gc.StatusNotOK,
			Response: err.Error(),
		}

		return gc.RaftError{Message: err.Error(), HttpCode: http.StatusInternalServerError}
	}

	index, err := r.appendLog(ctx, newLog)
	if err != nil {
		r.log().ErrorContext(ctx, "ClientRequest_appendLog", err)

		*output = gc.ClientRequestOutput{
			Status:     gc.StatusNotOK,
			Response:   "append log err: " + err.Error(),
			LeaderHint: "",
		}

		return gc.RaftError{Message: "append log err: " + err.Error(), HttpCode: http.StatusInternalServerError}
	}

	r.inOutLock.Unlock()

	span.AddEvent("log appended")

	var status gc.ClientRequestStatus = gc.StatusOK
	var response gc.LogResult

	if err := r.arm.Register(index); err != nil {
		r.log().ErrorContext(ctx, "ClientRequest_Register", err)

		*output = gc.ClientRequestOutput{
			Status:     gc.StatusNotOK,
			Response:   "can't register for async response" + err.Error(),
			LeaderHint: "",
		}

		return err
	}

	response, err = r.arm.TakeResponse(index, 30*time.Second)
	if err != nil {
		status = gc.StatusNotOK

		response = fmt.Sprintf("timeout: %s", err.Error())
		if errors.Is(err, common.ErrorSessionExpired) {
			response = gc.SessionExpired
		}

		r.log().ErrorContext(ctx, "ClientRequest_TakeResponse", err)
	}

	span.AddEvent("log committed")

	*output = gc.ClientRequestOutput{
		Status:   status,
		Response: response,
	}

	return err
}

func (r *RaftBrainImpl) RegisterClient(ctx context.Context, input gc.Log, output *gc.RegisterClientOutput) (err error) {
	ctx, span := tracer.Start(ctx, "RegisterClient")
	defer span.End()

	defer func() {
		if output.Status == gc.StatusNotOK {
			span.SetStatus(codes.Error, "not ok")
		} else {
			span.SetStatus(codes.Ok, "finished client request")
		}
	}()

	r.inOutLock.Lock()

	if r.state != gc.StateLeader {
		leaderUrl := r.getLeaderHttpUrl()

		*output = gc.RegisterClientOutput{
			Status:     gc.StatusNotOK,
			LeaderHint: leaderUrl,
			Response:   gc.NotLeader,
		}

		r.inOutLock.Unlock()

		return nil
	}

	newLog, err := r.logFactory.AttachTermAndTime(input, r.GetCurrentTerm(), r.clusterClock.LeaderStamp())
	if err != nil {
		*output = gc.RegisterClientOutput{
			Status:   gc.StatusNotOK,
			Response: err.Error(),
		}

		return nil
	}

	index, err := r.appendLog(ctx, newLog)
	if err != nil {
		r.log().ErrorContext(ctx, "RegisterClient_appendLog", err)

		*output = gc.RegisterClientOutput{
			Status:     gc.StatusNotOK,
			Response:   "append log err: " + err.Error(),
			LeaderHint: "",
		}
	}

	r.inOutLock.Unlock()

	var status gc.ClientRequestStatus = gc.StatusOK

	if err := r.arm.Register(index); err != nil {
		r.log().ErrorContext(ctx, "RegisterClient_Register", err)
		*output = gc.RegisterClientOutput{
			Status:     gc.StatusNotOK,
			LeaderHint: "",
			Response:   "error: can't register for async response. " + err.Error(),
		}

		return nil
	}

	_, err = r.arm.TakeResponse(index, 30*time.Second)
	if err != nil {
		r.log().ErrorContext(ctx, "RegisterClient_TakeResponse", err)

		*output = gc.RegisterClientOutput{
			Status:     gc.StatusNotOK,
			LeaderHint: "",
			Response:   "timeout: wait for async response. " + err.Error(),
		}

		return nil
	}

	*output = gc.RegisterClientOutput{
		Status:     status,
		Response:   strconv.Itoa(index),
		LeaderHint: "",
	}

	return nil
}

func (r *RaftBrainImpl) ClientQuery(ctx context.Context, input gc.Log, output *gc.ClientQueryOutput) (err error) {
	ctx, span := tracer.Start(ctx, "ClientQuery")
	defer span.End()

	defer func() {
		if err != nil {
			span.SetStatus(codes.Error, "")
		} else {
			span.SetStatus(codes.Ok, "query success")
		}
	}()

	if r.state != gc.StateLeader {
		leaderUrl := r.getLeaderHttpUrl()

		*output = gc.ClientQueryOutput{
			Status:     gc.StatusNotOK,
			Response:   gc.NotLeader,
			LeaderHint: leaderUrl,
		}

		return gc.RaftError{LeaderHint: leaderUrl, Message: gc.NotLeader, HttpCode: http.StatusMovedPermanently}
	}

	log := input
	// log, err := r.logFactory.AttachTermAndTime(input, r.GetCurrentTerm(), r.clusterClock.LeaderStamp())
	// if err != nil {
	// 	*output = gc.ClientQueryOutput{
	// 		Status:   common.StatusNotOK,
	// 		Response: err.Error(),
	// 	}

	// 	return nil
	// }

	var ok bool
	err = errors.New("timeout")
	for i := 0; i < 100; i++ {
		log, err := r.GetLog(r.commitIndex)
		if err != nil && !errors.Is(err, common.ErrLogIsInSnapshot) {
			break
		}

		if log.GetTerm() == r.persistState.GetCurrentTerm() {
			ok = true
			break
		}

		time.Sleep(r.heartBeatTimeOutMin)
	}

	if !ok {
		*output = gc.ClientQueryOutput{
			Status:     gc.StatusNotOK,
			Response:   "timeout: leader of current term haven't commit any log yet. " + err.Error(),
			LeaderHint: "",
		}

		return gc.RaftError{
			Message:  "timeout: leader of current term haven't commit any log yet. " + err.Error(),
			HttpCode: http.StatusInternalServerError,
		}
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
		*output = gc.ClientQueryOutput{
			Status:     gc.StatusNotOK,
			Response:   "timeout: wait for log committing",
			LeaderHint: "",
		}

		return gc.RaftError{
			Message:  "timeout: wait for log committing",
			HttpCode: http.StatusInternalServerError,
		}
	}

	res, err := r.stateMachine.Process(ctx, 0, log)
	if err != nil {
		*output = gc.ClientQueryOutput{
			Status:     gc.StatusNotOK,
			Response:   err.Error(),
			LeaderHint: "",
		}

		return err
	}

	*output = gc.ClientQueryOutput{
		Status:     gc.StatusOK,
		Response:   res,
		LeaderHint: "",
	}

	return nil
}
