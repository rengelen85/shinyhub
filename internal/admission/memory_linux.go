//go:build linux

package admission

func readMemoryLimit() (uint64, bool) {
	limit, ok, err := systemCgroups.MemoryLimit()
	return limit, ok && err == nil
}

func cgroupAvailableMemory(hostAvailable uint64) (uint64, error) {
	return systemCgroups.MemoryHeadroom(hostAvailable)
}
