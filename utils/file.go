package utils

import (
	"io/fs"
	"path/filepath"
	"syscall"
	"unsafe"
)

// return size of dir
func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func AvailableDiskSize() (uint64, error) {
	/*****Linux******/
	// wd, err := syscall.Getwd()
	// if err != nil {
	// 	return 0, err
	// }
	// var stat syscall.Statfs_t
	// if err = syscall.Statfs(wd, &stat); err != nil {
	// 	return 0, err
	// }
	// return stat.Bavail * uint64(stat.Bsize), nil

	/*****Windows******/
	kernel32, err := syscall.LoadLibrary("Kernel32.dll")
	if err != nil {
		return 0, err
	}
	defer syscall.FreeLibrary(kernel32)

	GetDiskFreeSpaceEx, err := syscall.GetProcAddress(syscall.Handle(kernel32), "GetDiskFreeSpaceExW")
	if err != nil {
		return 0, err
	}

	lpFreeBytesAvailable := uint64(0)
	lpTotalNumberOfBytes := uint64(0)
	lpTotalNumberOfFreeBytes := uint64(0)
	_, _, _ = syscall.Syscall6(uintptr(GetDiskFreeSpaceEx), 4,
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr("C:"))), // if on other disk, motify
		uintptr(unsafe.Pointer(&lpFreeBytesAvailable)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfBytes)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfFreeBytes)), 0, 0)

	return lpFreeBytesAvailable, nil
}
