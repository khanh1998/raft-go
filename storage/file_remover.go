package storage

type Storage interface {
	GetObjectNames() ([]string, error)
	DeleteObject(fileName string) error
}

// this will remove unnecessary WAL and snapshot files
type FileRemover struct {
	s Storage
}

func (f *FileRemover) CleanupWAL() {

}
