package common

import gc "khanh/raft-go/common"

type MockedSnapshot struct {
}

func (m MockedSnapshot) Metadata() gc.SnapshotMetadata {
	return gc.SnapshotMetadata{}
}
func (m MockedSnapshot) GetLastConfig() map[int]gc.ClusterMember {
	return map[int]gc.ClusterMember{}
}
func (m MockedSnapshot) Copy() gc.Snapshot {
	return &MockedSnapshot{}
}
func (m MockedSnapshot) Serialize() (data []byte) {
	return data
}
func (m MockedSnapshot) ToString() (data []string) {
	return
}
func (m *MockedSnapshot) FromString(data []string) error {
	return nil
}
func (m *MockedSnapshot) Deserialize(data []byte) error {
	return nil
}
