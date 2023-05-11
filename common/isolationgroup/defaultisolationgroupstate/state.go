// The MIT License (MIT)

// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package defaultisolationgroupstate

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/uber/cadence/common/isolationgroup/isolationgroupapi"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/dynamicconfig"
	"github.com/uber/cadence/common/isolationgroup"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/types"
)

type defaultIsolationGroupStateHandler struct {
	status                     int32
	done                       chan struct{}
	log                        log.Logger
	domainCache                cache.DomainCache
	globalIsolationGroupDrains dynamicconfig.Client
	config                     defaultConfig
}

// NewDefaultIsolationGroupStateWatcherWithConfigStoreClient Is a constructor which allows passing in the dynamic config client
func NewDefaultIsolationGroupStateWatcherWithConfigStoreClient(
	logger log.Logger,
	dc *dynamicconfig.Collection,
	domainCache cache.DomainCache,
	cfgStoreClient dynamicconfig.Client, // can be nil, which means global drain is unsupported
) (isolationgroup.State, error) {
	stopChan := make(chan struct{})

	allIGs := dc.GetListProperty(dynamicconfig.AllIsolationGroups)()
	allIsolationGroups, err := isolationgroupapi.MapAllIsolationGroupsResponse(allIGs)
	if err != nil {
		return nil, fmt.Errorf("could not get all isolation groups fron dynamic config: %w", err)
	}

	config := defaultConfig{
		IsolationGroupEnabled: dc.GetBoolPropertyFilteredByDomain(dynamicconfig.EnableTasklistIsolation),
		AllIsolationGroups:    allIsolationGroups,
	}

	return &defaultIsolationGroupStateHandler{
		done:                       stopChan,
		domainCache:                domainCache,
		globalIsolationGroupDrains: cfgStoreClient,
		status:                     common.DaemonStatusInitialized,
		log:                        logger,
		config:                     config,
	}, nil
}

func (z *defaultIsolationGroupStateHandler) AvailableIsolationGroupsByDomainID(ctx context.Context, domainID string, availableIsolationGroups []string) (types.IsolationGroupConfiguration, error) {
	state, err := z.getByDomainID(ctx, domainID)
	if err != nil {
		return nil, fmt.Errorf("unable to get isolation group state: %w", err)
	}
	isolationGroups := common.IntersectionStringSlice(z.config.AllIsolationGroups, availableIsolationGroups)
	return availableIG(isolationGroups, state.Global, state.Domain), nil
}

func (z *defaultIsolationGroupStateHandler) IsDrained(ctx context.Context, domain string, isolationGroup string) (bool, error) {
	state, err := z.get(ctx, domain)
	if err != nil {
		return false, fmt.Errorf("could not determine if drained: %w", err)
	}
	return isDrained(isolationGroup, state.Global, state.Domain), nil
}

func (z *defaultIsolationGroupStateHandler) IsDrainedByDomainID(ctx context.Context, domainID string, isolationGroup string) (bool, error) {
	domain, err := z.domainCache.GetDomainByID(domainID)
	if err != nil {
		return false, fmt.Errorf("could not determine if drained: %w", err)
	}
	return z.IsDrained(ctx, domain.GetInfo().Name, isolationGroup)
}

// Start the state handler
func (z *defaultIsolationGroupStateHandler) Start() {
	if !atomic.CompareAndSwapInt32(&z.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
}

func (z *defaultIsolationGroupStateHandler) Stop() {
	if z == nil {
		return
	}
	if !atomic.CompareAndSwapInt32(&z.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}
	close(z.done)
}

func (z *defaultIsolationGroupStateHandler) getByDomainID(ctx context.Context, domainID string) (*isolationGroups, error) {
	domain, err := z.domainCache.GetDomainByID(domainID)
	if err != nil {
		return nil, fmt.Errorf("could not resolve domain in isolationGroup handler: %w", err)
	}
	return z.get(ctx, domain.GetInfo().Name)
}

// Get the statue of a isolationGroup, with respect to both domain and global drains. Domain-specific drains override global config
// will return nil, nil when it is not enabled
func (z *defaultIsolationGroupStateHandler) get(ctx context.Context, domain string) (*isolationGroups, error) {
	if !z.config.IsolationGroupEnabled(domain) {
		return nil, nil
	}

	domainData, err := z.domainCache.GetDomain(domain)
	if err != nil {
		return nil, fmt.Errorf("could not resolve domain in isolationGroup handler: %w", err)
	}

	domainCfg := domainData.GetConfig()
	var domainState types.IsolationGroupConfiguration
	if domainCfg != nil && domainCfg.IsolationGroups != nil {
		domainState = domainCfg.IsolationGroups
	}
	ig := &isolationGroups{
		Domain: domainState,
	}

	if z.globalIsolationGroupDrains != nil {
		globalCfg, err := z.globalIsolationGroupDrains.GetListValue(dynamicconfig.DefaultIsolationGroupConfigStoreManagerGlobalMapping, nil)
		if err != nil {
			return nil, fmt.Errorf("could not resolve global drains in %w", err)
		}

		globalState, err := isolationgroupapi.MapDynamicConfigResponse(globalCfg)
		if err != nil {
			return nil, fmt.Errorf("could not resolve global drains in isolationGroup handler: %w", err)
		}
		ig.Global = globalState
	}

	return ig, nil
}

// A simple explicit deny-based isolation group implementation
func availableIG(allIsolationGroups []string, global types.IsolationGroupConfiguration, domain types.IsolationGroupConfiguration) types.IsolationGroupConfiguration {
	out := types.IsolationGroupConfiguration{}
	for _, isolationGroup := range allIsolationGroups {
		globalCfg, hasGlobalConfig := global[isolationGroup]
		domainCfg, hasDomainConfig := domain[isolationGroup]
		if hasGlobalConfig {
			if globalCfg.State == types.IsolationGroupStateDrained {
				continue
			}
		}
		if hasDomainConfig {
			if domainCfg.State == types.IsolationGroupStateDrained {
				continue
			}
		}
		out[isolationGroup] = types.IsolationGroupPartition{
			Name:  isolationGroup,
			State: types.IsolationGroupStateHealthy,
		}
	}
	return out
}

func isDrained(isolationGroup string, global types.IsolationGroupConfiguration, domain types.IsolationGroupConfiguration) bool {
	globalCfg, hasGlobalConfig := global[isolationGroup]
	domainCfg, hasDomainConfig := domain[isolationGroup]
	if hasGlobalConfig {
		if globalCfg.State == types.IsolationGroupStateDrained {
			return true
		}
	}
	if hasDomainConfig {
		if domainCfg.State == types.IsolationGroupStateDrained {
			return true
		}
	}
	return false
}