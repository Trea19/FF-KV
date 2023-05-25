package fio

const DataFilePerm = 0644

type FileIOType byte

const (
	StandardFIO FileIOType = iota
	MemoryMap
)

type IOManager interface {
	Read([]byte, int64) (int, error) //read data from the given pos of the file
	Write([]byte) (int, error)       //write []byte into file
	Sync() error                     //persist data
	Close() error                    //close file
	Size() (int64, error)            //get file size
}

// initialize IOManger, support standard FileIO / MMap
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}
}
