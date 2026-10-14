package admission_test

import (
	"testing"
	"testing/fstest"

	"github.com/rvben/shinyhub/internal/admission"
)

func TestCgroupReaderUsesEveryVisibleAncestor(t *testing.T) {
	files := fstest.MapFS{}
	for name, content := range map[string]string{
		"proc/self/cgroup":                                   "0::/slice/service/_supervisor\n",
		"proc/self/mountinfo":                                "36 25 0:32 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n",
		"sys/fs/cgroup/cpu.max":                              "800000 100000",
		"sys/fs/cgroup/cpu.stat":                             "usage_usec 9000000\n",
		"sys/fs/cgroup/slice/cpu.max":                        "50000 100000",
		"sys/fs/cgroup/slice/cpu.stat":                       "usage_usec 7000000\n",
		"sys/fs/cgroup/slice/service/cpu.max":                "100000 100000",
		"sys/fs/cgroup/slice/service/cpu.stat":               "usage_usec 3000000\n",
		"sys/fs/cgroup/slice/service/_supervisor/cpu.max":    "max 100000",
		"sys/fs/cgroup/memory.max":                           "4294967296",
		"sys/fs/cgroup/memory.current":                       "4160749568",
		"sys/fs/cgroup/slice/service/memory.max":             "1073741824",
		"sys/fs/cgroup/slice/service/memory.current":         "268435456",
		"sys/fs/cgroup/slice/service/_supervisor/memory.max": "max",
	} {
		files[name] = &fstest.MapFile{Data: []byte(content)}
	}
	reader := admission.NewCgroupReader(files)
	cores, limited, err := reader.CPUCapacity()
	if err != nil || !limited || cores != 0.5 {
		t.Fatalf("ancestor CPU capacity: %v %v %v", cores, limited, err)
	}
	available, err := reader.MemoryHeadroom(8 << 30)
	if err != nil || available != 128<<20 {
		t.Fatalf("shared ancestor memory headroom: %v %v", available, err)
	}
}

func TestCgroupReaderFindsV1ControllerOnHybridMount(t *testing.T) {
	files := fstest.MapFS{}
	for name, content := range map[string]string{
		"proc/self/cgroup":                            "0::/service\n2:cpu:/service\n3:cpuacct:/service\n",
		"proc/self/mountinfo":                         "36 25 0:32 / /sys/fs/cgroup/unified rw - cgroup2 cgroup rw\n37 25 0:33 / /sys/fs/cgroup/cpu rw - cgroup cgroup rw,cpu\n38 25 0:34 / /sys/fs/cgroup/cpuacct rw - cgroup cgroup rw,cpuacct\n",
		"sys/fs/cgroup/unified/service/cgroup.procs":  "1",
		"sys/fs/cgroup/cpu/service/cpu.cfs_quota_us":  "150000",
		"sys/fs/cgroup/cpu/service/cpu.cfs_period_us": "100000",
		"sys/fs/cgroup/cpu/cpu.cfs_quota_us":          "-1",
		"sys/fs/cgroup/cpuacct/service/cpuacct.usage": "123000000",
	} {
		files[name] = &fstest.MapFile{Data: []byte(content)}
	}
	cores, limited, err := admission.NewCgroupReader(files).CPUCapacity()
	if err != nil || !limited || cores != 1.5 {
		t.Fatalf("hybrid CPU capacity = %v %v %v, want 1.5", cores, limited, err)
	}
}

func TestCgroupReaderResolvesNamespaceAndEscapedMountRoots(t *testing.T) {
	for _, membership := range []string{"/tenant/service", "/service"} {
		t.Run(membership, func(t *testing.T) {
			files := fstest.MapFS{
				"proc/self/cgroup":                    {Data: []byte("0::" + membership + "\n")},
				"proc/self/mountinfo":                 {Data: []byte("36 25 0:32 /tenant /custom\\040mount rw - cgroup2 cgroup rw\n")},
				"custom mount/service/cpu.max":        {Data: []byte("25000 100000")},
				"custom mount/service/cpu.stat":       {Data: []byte("usage_usec 1\n")},
				"custom mount/service/memory.max":     {Data: []byte("67108864")},
				"custom mount/service/memory.current": {Data: []byte("16777216")},
			}
			reader := admission.NewCgroupReader(files)
			cores, limited, err := reader.CPUCapacity()
			if err != nil || !limited || cores != 0.25 {
				t.Fatalf("namespace CPU capacity: %v %v %v", cores, limited, err)
			}
			remaining, err := reader.MemoryHeadroom(1 << 30)
			if err != nil || remaining != 48<<20 {
				t.Fatalf("namespace memory headroom: %v %v", remaining, err)
			}
		})
	}
}

func TestCgroupReaderRefusesInvalidOrMissingAccounting(t *testing.T) {
	for _, tc := range []struct {
		name, file, value string
		memory            bool
	}{
		{"invalid quota", "cpu.max", "NaN 100000", false},
		{"zero period", "cpu.max", "50000 0", false},
		{"invalid usage", "cpu.stat", "usage_usec NaN", false},
		{"missing usage", "cpu.stat", "", false},
		{"invalid memory limit", "memory.max", "garbage", true},
		{"missing memory usage", "memory.current", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := fstest.MapFS{
				"proc/self/cgroup":                     {Data: []byte("0::/service\n")},
				"proc/self/mountinfo":                  {Data: []byte("36 25 0:32 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n")},
				"sys/fs/cgroup/service/cpu.max":        {Data: []byte("50000 100000")},
				"sys/fs/cgroup/service/cpu.stat":       {Data: []byte("usage_usec 1")},
				"sys/fs/cgroup/service/memory.max":     {Data: []byte("67108864")},
				"sys/fs/cgroup/service/memory.current": {Data: []byte("16777216")},
			}
			name := "sys/fs/cgroup/service/" + tc.file
			if tc.value == "" {
				delete(files, name)
			} else {
				files[name] = &fstest.MapFile{Data: []byte(tc.value)}
			}
			reader := admission.NewCgroupReader(files)
			var err error
			if tc.memory {
				_, err = reader.MemoryHeadroom(1 << 30)
			} else {
				_, _, err = reader.CPUCapacity()
			}
			if err == nil {
				t.Fatal("invalid accounting was treated as available capacity")
			}
		})
	}
}
