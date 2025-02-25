// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// planner core metrics vars
var (
	PseudoEstimationNotAvailable            prometheus.Counter
	PseudoEstimationOutdate                 prometheus.Counter
	preparedPlanCacheHitCounter             prometheus.Counter
	nonPreparedPlanCacheHitCounter          prometheus.Counter
	preparedPlanCacheMissCounter            prometheus.Counter
	nonPreparedPlanCacheMissCounter         prometheus.Counter
	nonPreparedPlanCacheUnsupportedCounter  prometheus.Counter
	sessionPlanCacheInstancePlanNumCounter  prometheus.Gauge
	sessionPlanCacheInstanceMemoryUsage     prometheus.Gauge
	instancePlanCacheInstancePlanNumCounter prometheus.Gauge
	instancePlanCacheInstanceMemoryUsage    prometheus.Gauge
	instancePlanCacheInstanceNumEvict       prometheus.Gauge
	sessionPlanCacheLookupDuration          prometheus.Observer
	instancePlanCacheLookupDuration         prometheus.Observer
	instancePlanCacheCloneDuration          prometheus.Observer
)

func init() {
	InitMetricsVars()
}

// InitMetricsVars init planner core metrics vars.
func InitMetricsVars() {
	PseudoEstimationNotAvailable = metrics.PseudoEstimation.WithLabelValues("nodata")
	PseudoEstimationOutdate = metrics.PseudoEstimation.WithLabelValues("outdate")
	// plan cache metrics
	preparedPlanCacheHitCounter = metrics.PlanCacheCounter.WithLabelValues("prepared")
	nonPreparedPlanCacheHitCounter = metrics.PlanCacheCounter.WithLabelValues("non-prepared")
	preparedPlanCacheMissCounter = metrics.PlanCacheMissCounter.WithLabelValues("prepared")
	nonPreparedPlanCacheMissCounter = metrics.PlanCacheMissCounter.WithLabelValues("non-prepared")
	nonPreparedPlanCacheUnsupportedCounter = metrics.PlanCacheMissCounter.WithLabelValues("non-prepared-unsupported")
	sessionPlanCacheInstancePlanNumCounter = metrics.PlanCacheInstancePlanNumCounter.WithLabelValues(" session-plan-cache")
	sessionPlanCacheInstanceMemoryUsage = metrics.PlanCacheInstanceMemoryUsage.WithLabelValues(" session-plan-cache")
	instancePlanCacheInstancePlanNumCounter = metrics.PlanCacheInstancePlanNumCounter.WithLabelValues(" instance-plan-cache")
	instancePlanCacheInstanceMemoryUsage = metrics.PlanCacheInstanceMemoryUsage.WithLabelValues(" instance-plan-cache")
	instancePlanCacheInstanceNumEvict = metrics.PlanCacheInstancePlanNumCounter.WithLabelValues(" instance-plan-cache-last-evict")
	sessionPlanCacheLookupDuration = metrics.PlanCacheProcessDuration.WithLabelValues(" session-plan-cache-lookup")
	instancePlanCacheLookupDuration = metrics.PlanCacheProcessDuration.WithLabelValues(" instance-plan-cache-lookup")
	instancePlanCacheCloneDuration = metrics.PlanCacheProcessDuration.WithLabelValues(" instance-plan-cache-clone")
}

// GetPlanCacheHitCounter get different plan cache hit counter
func GetPlanCacheHitCounter(isNonPrepared bool) prometheus.Counter {
	if isNonPrepared {
		return nonPreparedPlanCacheHitCounter
	}
	return preparedPlanCacheHitCounter
}

// GetPlanCacheMissCounter get different plan cache miss counter
func GetPlanCacheMissCounter(isNonPrepared bool) prometheus.Counter {
	if isNonPrepared {
		return nonPreparedPlanCacheMissCounter
	}
	return preparedPlanCacheMissCounter
}

// GetNonPrepPlanCacheUnsupportedCounter get non-prepared plan cache unsupported counter.
func GetNonPrepPlanCacheUnsupportedCounter() prometheus.Counter {
	return nonPreparedPlanCacheUnsupportedCounter
}

// GetPlanCacheInstanceNumCounter get different plan counter of plan cache
func GetPlanCacheInstanceNumCounter(instancePlanCache bool) prometheus.Gauge {
	if instancePlanCache {
		return instancePlanCacheInstancePlanNumCounter
	}
	return sessionPlanCacheInstancePlanNumCounter
}

// GetPlanCacheInstanceMemoryUsage get different plan memory usage counter of plan cache
func GetPlanCacheInstanceMemoryUsage(instancePlanCache bool) prometheus.Gauge {
	if instancePlanCache {
		return instancePlanCacheInstanceMemoryUsage
	}
	return sessionPlanCacheInstanceMemoryUsage
}

// GetPlanCacheCloneDuration get different plan cache clone duration.
func GetPlanCacheCloneDuration() prometheus.Observer {
	return instancePlanCacheCloneDuration
}

// GetPlanCacheLookupDuration get different plan cache lookup duration.
func GetPlanCacheLookupDuration(instancePlanCache bool) prometheus.Observer {
	if instancePlanCache {
		return instancePlanCacheLookupDuration
	}
	return sessionPlanCacheLookupDuration
}

// GetPlanCacheInstanceEvict get instance plan cache evict counter.
func GetPlanCacheInstanceEvict() prometheus.Gauge {
	return instancePlanCacheInstanceNumEvict
}
