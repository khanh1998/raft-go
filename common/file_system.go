package common

import (
	"os"
)

func CreateFolderIfNotExists(folder string) (err error) {
	_, err = os.Stat(folder)
	if os.IsNotExist(err) {
		err = os.MkdirAll(folder, 0755)
		if err != nil {
			return err
		}
	}
	return err
}

func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	if err == nil {
		return true
	}

	if os.IsNotExist(err) {
		return false
	}

	return false
}
