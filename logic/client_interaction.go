package logic

import (
	"khanh/raft-go/common"
	"net/url"
)

func (r *RaftBrainImpl) ClientRequest(input *common.ClientQueryInput, output *common.ClientQueryOutput) (err error) {
	//
	if r.State != StateLeader {
		*output = common.ClientQueryOutput{
			Status:     common.StatusNotOK,
			Response:   common.NotLeader,
			LeaderHint: url.URL{},
		}

		return nil
	}

	return nil
}

func (r *RaftBrainImpl) RegisterClient(input *common.RegisterClientInput, output *common.RegisterClientOutput) (err error) {

	return nil
}

func (r *RaftBrainImpl) ClientQuery(input *common.ClientQueryInput, output *common.ClientQueryOutput) (err error) {

	return nil
}
