package api

import (
	"fmt"
	"testing/fstest"

	"github.com/rvben/shinyhub/internal/admission"
)

// Lifecycle fixtures model worker budgets alongside their headroom sampler,
// rather than inheriting unrelated CI quotas. Native load tests exercise real
// pressure and the default kernel reader.
func setTestCPUQuota(srv *Server, cores float64) {
	srv.SetCPUQuotaReaderForTest(admission.NewCgroupReader(fstest.MapFS{
		"proc/self/cgroup":       {Data: []byte("0::/\n")},
		"proc/self/mountinfo":    {Data: []byte("1 0 0:1 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n")},
		"sys/fs/cgroup/cpu.max":  {Data: []byte(fmt.Sprintf("%.0f 100000\n", cores*100000))},
		"sys/fs/cgroup/cpu.stat": {Data: []byte("usage_usec 0\n")},
	}))
}
