//go:build darwin

package engine

/*
#include <mach/mach.h>

static long long cappy_resident_size(void) {
	mach_task_basic_info_data_t info;
	mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
	if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &count) != KERN_SUCCESS) {
		return -1;
	}
	return (long long)info.resident_size;
}
*/
import "C"

import "syscall"

// currentRSS is the process's resident set size now, from mach task_info.
func currentRSS() int64 {
	return int64(C.cappy_resident_size())
}

// maxRSS is the peak resident set size (getrusage; bytes on macOS).
func maxRSS() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return -1
	}
	return ru.Maxrss
}
