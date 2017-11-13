package scheduler

import (
	"bytes"
	"fmt"
	"github.com/Sirupsen/logrus"
	"sort"
	"strconv"
)

// ComputeFilter define a filter based on cpu, memory and instance number
type ComputeFilter struct {
}

type Pair struct {
	gpuUsed int64
	index   int
}

type Pairs []Pair

func (pairs Pairs) Len() int {
	return len(pairs)
}

func (pairs Pairs) Swap(i, j int) {
	pairs[i], pairs[j] = pairs[j], pairs[i]
}

func (pairs Pairs) Less(i, j int) bool {
	return pairs[i].gpuUsed < pairs[j].gpuUsed
}

func (c ComputeFilter) Filter(scheduler *Scheduler, resourceRequests []ResourceRequest, context Context, hosts []string) []string {
	filteredHosts := filter(scheduler.hosts, resourceRequests)
	result := []string{}
	for _, host := range filteredHosts {

		result = append(result, host.id)

		//result = append(result, host.id)
	}
	return result
}

// ComputeReserveAction is a reserve action for cpu, memory, instance count and other compute resource.
type ComputeReserveAction struct {
	offset int
}

func (c *ComputeReserveAction) Reserve(scheduler *Scheduler, requests []ResourceRequest, context Context, host *host, force bool, data map[string]interface{}) error {
	var err error
	var reserveLog *bytes.Buffer

	firstSet := false
	for _, rr := range requests {
		if rr.GetResourceType() == "instanceReservation" {
			firstSet = true
			break
		}
	}

	if context != nil && len(context) > 0 && firstSet {
		if gpuStr, ok := context[0].Data.Fields.Labels["gpu"]; ok {
			var gpuRatio int64 = 1
			if ratioStr, ratioOk := context[0].Data.Fields.Labels["ratio"]; ratioOk {
				if ratio, err := strconv.ParseInt(ratioStr, 10, 64); err == nil {
					gpuRatio = ratio
				}
			}

			if gpuNeeded, err := strconv.ParseInt(gpuStr, 10, 64); err == nil {
				if gpuPool, ok := host.pools["gpuReservation"]; ok {
					tempPairs := make(Pairs, int(gpuPool.(*ComputeResourcePool).Total/10))
					for i := 0; i < len(tempPairs); i++ {
						gpuCardName := "gpu-card" + strconv.Itoa(i)
						tempPairs[i] = Pair{host.pools[gpuCardName].(*ComputeResourcePool).Used, i}
					}
					sort.Sort(tempPairs)

					for i := 0; i < int(gpuNeeded); i++ {
						gpuCardName := "gpu-card" + strconv.Itoa(tempPairs[i].index)
						gpuRequest := AmountBasedResourceRequest{gpuCardName, gpuRatio}
						requests = append(requests, gpuRequest)
					}
				}
			}
		}
	}

	for _, rr := range requests {
		p, ok := host.pools[rr.GetResourceType()]
		if !ok {
			logrus.Warnf("Pool %v for host %v not found for reserving %v. Skipping reservation", rr.GetResourceType(), host.id, rr)
			continue
		}
		PoolType := p.GetPoolType()
		if PoolType == computePool {
			pool := p.(*ComputeResourcePool)
			request := rr.(AmountBasedResourceRequest)
			if !force && pool.Used+request.Amount > pool.Total {
				err = OverReserveError{hostID: host.id, resourceRequest: rr}
				return err
			}

			pool.Used = pool.Used + request.Amount
			c.offset = c.offset + 1
			if reserveLog == nil {
				reserveLog = bytes.NewBufferString(fmt.Sprintf("New pool amount on host %v:", host.id))
			}
			reserveLog.WriteString(fmt.Sprintf(" %v total: %v used: %v.", request.Resource, pool.Total, pool.Used))
		}
	}
	if reserveLog != nil {
		logrus.Info(reserveLog.String())
	}
	return nil
}

func (c *ComputeReserveAction) RollBack(scheduler *Scheduler, requests []ResourceRequest, context Context, host *host) {
	for _, rr := range requests[:c.offset] {
		p, ok := host.pools[rr.GetResourceType()]
		if !ok {
			break
		}
		resourcePoolType := p.GetPoolType()
		if resourcePoolType == computePool {
			pool := p.(*ComputeResourcePool)
			request := rr.(AmountBasedResourceRequest)
			pool.Used = pool.Used - request.Amount
		}
	}
	c.offset = 0
}

// ComputeReleaseAction is a release action to release cpu, memory, instance counts and other compute resources
type ComputeReleaseAction struct{}

func (c ComputeReleaseAction) Release(scheduler *Scheduler, requests []ResourceRequest, context Context, host *host) {
	releaseLog := bytes.NewBufferString(fmt.Sprintf("New pool amounts on host %v:", host.id))
	for _, rr := range requests {
		p, ok := host.pools[rr.GetResourceType()]
		if !ok {
			logrus.Infof("Host %v doesn't have resource pool %v. Nothing to do.", host.id, rr.GetResourceType())
			continue
		}
		PoolType := p.GetPoolType()
		if PoolType == computePool {
			pool := p.(*ComputeResourcePool)
			request := rr.(AmountBasedResourceRequest)
			if pool.Used-request.Amount < 0 {
				logrus.Infof("Decreasing used for %v.%v by %v would result in negative usage. Setting to 0.", host.id, request.Resource, request.Amount)
				pool.Used = 0
			} else {
				pool.Used = pool.Used - request.Amount
			}
			releaseLog.WriteString(fmt.Sprintf(" %v total: %v used: %v.", request.Resource, pool.Total, pool.Used))
		}
	}
	logrus.Info(releaseLog.String())
}

type OverReserveError struct {
	hostID          string
	resourceRequest ResourceRequest
}

func (e OverReserveError) Error() string {
	return fmt.Sprintf("Not enough available resources on host %v to reserve %v.", e.hostID, e.resourceRequest)
}
