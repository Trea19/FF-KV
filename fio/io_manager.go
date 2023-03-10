package fio

const DataFilePerm = 0644

type IOManager interface {
	Read([]byte, int64) (int, error) //read data from the given pos of the file
	Write([]byte) (int, error)       //write []byte into file
	Sync() error                     //persist data
	Close() error                    //close file
}
