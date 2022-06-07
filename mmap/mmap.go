package mmap

import (
	"os"
	"reflect"
	"unsafe"
)

func Mmap(file *os.File, writable bool, size int) ([]byte, error) {
	mType := unix.PROT_READ //映射可读区
	if writable {           //映射可写区
		mType |= unix.PROT_WRITE
	}
	//MAP_SHARED指定了进程对内存区域的修改会影响到映射文件。
	return unix.Mmap(int(file.Fd()), 0, size, mType, unix.MAP_SHARED)
}

//munmap 释放由mmap创建的这段内存空间
//int munmap(void *start, size_t length);
//前者是内存映射的起始地址,后者是内存映射的长度 ;
//munmap函数成功返回0.失败返回-1并设置errno
// unix.Munmap maintains an internal list of mmapped addresses, and only calls munmap
// if the address is present in that list. If we use mremap, this list is not updated.
// To bypass this, we call munmap ourselves.
func Munmap(data []byte) error {
	if len(data) == 0 || len(data) != cap(data) {
		return unix.EINVAL
	}
	_, _, err := unix.Syscall(
		unix.SYS_MUNMAP,
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(len(data)),
		0,
	)
	if err != 0 {
		return err
	}
	return nil
}

//扩大/缩小现有内存映射，flags参数还可以控制是否需要页对齐
//成功，返回一个指向新虚拟内存区域的指针。
//失败，返回MAP_FAILED。
//函数原型void * mremap(void *old_address, size_t old_size , size_t new_size, int flags.../* void *new_address */);
func Mremap(data []byte, size int) error {
	const MREMAP_MAYMOVE = 0x1 //允许内核将映射重定位到新的虚拟地址,没有足够空间expand，mremap()失败

	sh := (*reflect.SliceHeader)(unsafe.Pointer(&data)) //底层数组的内存地址
	newAddr, _, err := unix.Syscall6(
		unix.SYS_MREMAP,
		header.Data,     //旧地址已经被page aligned页对齐
		uintptr(sh.Len), //VMB虚拟内存块的大小
		uintptr(size),   //mremap操作后需要的VMB大小
		uintptr(MREMAP_MAYMOVE),
		0,
		0,
	)
	if err != 0 {
		return nil, err
	}

	sh.Data = newAddr
	sh.Cap = size
	sh.Len = size
	return nil
}

//把在该内存段的某个部分或者整段中的修改写回到被映射的文件中（或者从被映射文件里读出）。
//int msync(void* addr, size_t len, int flags);
func Msync(b []byte) error {
	//MS_SYNC采用同步写方式
	return unix.Msync(b, unix.MS_SYNC)
}
