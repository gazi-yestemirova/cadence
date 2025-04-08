// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
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

package worker

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/constants"
	"github.com/uber/cadence/common/domain"
	"github.com/uber/cadence/common/dynamicconfig"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/resource"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/types"
	"github.com/uber/cadence/service/worker/archiver"
	"github.com/uber/cadence/service/worker/asyncworkflow"
	"github.com/uber/cadence/service/worker/batcher"
	"github.com/uber/cadence/service/worker/diagnostics"
	"github.com/uber/cadence/service/worker/domaindeprecation"
	"github.com/uber/cadence/service/worker/esanalyzer"
	"github.com/uber/cadence/service/worker/failovermanager"
	"github.com/uber/cadence/service/worker/indexer"
	"github.com/uber/cadence/service/worker/parentclosepolicy"
	"github.com/uber/cadence/service/worker/replicator"
	"github.com/uber/cadence/service/worker/scanner"
	"github.com/uber/cadence/service/worker/scanner/executions"
	"github.com/uber/cadence/service/worker/scanner/shardscanner"
	"github.com/uber/cadence/service/worker/scanner/tasklist"
	"github.com/uber/cadence/service/worker/scanner/timers"
)

type (
	// Service represents the cadence-worker service. This service hosts all background processing needed for cadence cluster:
	// 1. Replicator: Handles applying replication tasks generated by remote clusters.
	// 2. Indexer: Handles uploading of visibility records to elastic search.
	// 3. Archiver: Handles archival of workflow histories.
	Service struct {
		resource.Resource

		status int32
		stopC  chan struct{}
		params *resource.Params
		config *Config
	}

	// Config contains all the service config for worker
	Config struct {
		AdminOperationToken                 dynamicconfig.StringPropertyFn
		KafkaCfg                            config.KafkaConfig
		ArchiverConfig                      *archiver.Config
		IndexerCfg                          *indexer.Config
		ScannerCfg                          *scanner.Config
		BatcherCfg                          *batcher.Config
		ESAnalyzerCfg                       *esanalyzer.Config
		failoverManagerCfg                  *failovermanager.Config
		ThrottledLogRPS                     dynamicproperties.IntPropertyFn
		PersistenceGlobalMaxQPS             dynamicproperties.IntPropertyFn
		PersistenceMaxQPS                   dynamicproperties.IntPropertyFn
		EnableBatcher                       dynamicproperties.BoolPropertyFn
		EnableParentClosePolicyWorker       dynamicproperties.BoolPropertyFn
		NumParentClosePolicySystemWorkflows dynamicproperties.IntPropertyFn
		EnableFailoverManager               dynamicproperties.BoolPropertyFn
		DomainReplicationMaxRetryDuration   dynamicproperties.DurationPropertyFn
		EnableESAnalyzer                    dynamicproperties.BoolPropertyFn
		EnableAsyncWorkflowConsumption      dynamicproperties.BoolPropertyFn
		HostName                            string
	}
)

// NewService builds a new cadence-worker service
func NewService(params *resource.Params) (resource.Resource, error) {
	serviceConfig := NewConfig(params)
	serviceResource, err := resource.New(
		params,
		service.Worker,
		&service.Config{
			PersistenceMaxQPS:        serviceConfig.PersistenceMaxQPS,
			PersistenceGlobalMaxQPS:  serviceConfig.PersistenceGlobalMaxQPS,
			ThrottledLoggerMaxRPS:    serviceConfig.ThrottledLogRPS,
			IsErrorRetryableFunction: common.IsServiceTransientError,
			// worker service doesn't need visibility config as it never call visibilityManager API
		},
	)
	if err != nil {
		return nil, err
	}

	return &Service{
		Resource: serviceResource,
		status:   common.DaemonStatusInitialized,
		config:   serviceConfig,
		params:   params,
		stopC:    make(chan struct{}),
	}, nil
}

// NewConfig builds the new Config for cadence-worker service
func NewConfig(params *resource.Params) *Config {
	dc := dynamicconfig.NewCollection(
		params.DynamicConfig,
		params.Logger,
		dynamicproperties.ClusterNameFilter(params.ClusterMetadata.GetCurrentClusterName()),
	)
	config := &Config{
		AdminOperationToken: dc.GetStringProperty(dynamicconfig.AdminOperationToken),
		ArchiverConfig: &archiver.Config{
			ArchiverConcurrency:             dc.GetIntProperty(dynamicproperties.WorkerArchiverConcurrency),
			ArchivalsPerIteration:           dc.GetIntProperty(dynamicproperties.WorkerArchivalsPerIteration),
			TimeLimitPerArchivalIteration:   dc.GetDurationProperty(dynamicproperties.WorkerTimeLimitPerArchivalIteration),
			AllowArchivingIncompleteHistory: dc.GetBoolProperty(dynamicproperties.AllowArchivingIncompleteHistory),
		},
		ScannerCfg: &scanner.Config{
			ScannerPersistenceMaxQPS: dc.GetIntProperty(dynamicproperties.ScannerPersistenceMaxQPS),
			TaskListScannerOptions: tasklist.Options{
				GetOrphanTasksPageSizeFn: dc.GetIntProperty(dynamicproperties.ScannerGetOrphanTasksPageSize),
				TaskBatchSizeFn:          dc.GetIntProperty(dynamicproperties.ScannerBatchSizeForTasklistHandler),
				EnableCleaning:           dc.GetBoolProperty(dynamicproperties.EnableCleaningOrphanTaskInTasklistScavenger),
				MaxTasksPerJobFn:         dc.GetIntProperty(dynamicproperties.ScannerMaxTasksProcessedPerTasklistJob),
			},
			Persistence:            &params.PersistenceConfig,
			ClusterMetadata:        params.ClusterMetadata,
			TaskListScannerEnabled: dc.GetBoolProperty(dynamicproperties.TaskListScannerEnabled),
			HistoryScannerEnabled:  dc.GetBoolProperty(dynamicproperties.HistoryScannerEnabled),
			ShardScanners: []*shardscanner.ScannerConfig{
				executions.ConcreteExecutionConfig(dc),
				executions.CurrentExecutionConfig(dc),
				timers.ScannerConfig(dc),
			},
			MaxWorkflowRetentionInDays: dc.GetIntProperty(dynamicproperties.MaxRetentionDays),
		},
		KafkaCfg: params.KafkaConfig,
		BatcherCfg: &batcher.Config{
			AdminOperationToken: dc.GetStringProperty(dynamicproperties.AdminOperationToken),
			ClusterMetadata:     params.ClusterMetadata,
		},
		failoverManagerCfg: &failovermanager.Config{
			AdminOperationToken: dc.GetStringProperty(dynamicproperties.AdminOperationToken),
			ClusterMetadata:     params.ClusterMetadata,
		},
		ESAnalyzerCfg: &esanalyzer.Config{
			ESAnalyzerPause:                          dc.GetBoolProperty(dynamicproperties.ESAnalyzerPause),
			ESAnalyzerTimeWindow:                     dc.GetDurationProperty(dynamicproperties.ESAnalyzerTimeWindow),
			ESAnalyzerMaxNumDomains:                  dc.GetIntProperty(dynamicproperties.ESAnalyzerMaxNumDomains),
			ESAnalyzerMaxNumWorkflowTypes:            dc.GetIntProperty(dynamicproperties.ESAnalyzerMaxNumWorkflowTypes),
			ESAnalyzerLimitToTypes:                   dc.GetStringProperty(dynamicproperties.ESAnalyzerLimitToTypes),
			ESAnalyzerEnableAvgDurationBasedChecks:   dc.GetBoolProperty(dynamicproperties.ESAnalyzerEnableAvgDurationBasedChecks),
			ESAnalyzerLimitToDomains:                 dc.GetStringProperty(dynamicproperties.ESAnalyzerLimitToDomains),
			ESAnalyzerNumWorkflowsToRefresh:          dc.GetIntPropertyFilteredByWorkflowType(dynamicproperties.ESAnalyzerNumWorkflowsToRefresh),
			ESAnalyzerBufferWaitTime:                 dc.GetDurationPropertyFilteredByWorkflowType(dynamicproperties.ESAnalyzerBufferWaitTime),
			ESAnalyzerMinNumWorkflowsForAvg:          dc.GetIntPropertyFilteredByWorkflowType(dynamicproperties.ESAnalyzerMinNumWorkflowsForAvg),
			ESAnalyzerWorkflowDurationWarnThresholds: dc.GetStringProperty(dynamicproperties.ESAnalyzerWorkflowDurationWarnThresholds),
			ESAnalyzerWorkflowVersionDomains:         dc.GetStringProperty(dynamicproperties.ESAnalyzerWorkflowVersionMetricDomains),
			ESAnalyzerWorkflowTypeDomains:            dc.GetStringProperty(dynamicproperties.ESAnalyzerWorkflowTypeMetricDomains),
		},
		EnableBatcher:                       dc.GetBoolProperty(dynamicproperties.EnableBatcher),
		EnableParentClosePolicyWorker:       dc.GetBoolProperty(dynamicproperties.EnableParentClosePolicyWorker),
		NumParentClosePolicySystemWorkflows: dc.GetIntProperty(dynamicproperties.NumParentClosePolicySystemWorkflows),
		EnableESAnalyzer:                    dc.GetBoolProperty(dynamicproperties.EnableESAnalyzer),
		EnableFailoverManager:               dc.GetBoolProperty(dynamicproperties.EnableFailoverManager),
		ThrottledLogRPS:                     dc.GetIntProperty(dynamicproperties.WorkerThrottledLogRPS),
		PersistenceGlobalMaxQPS:             dc.GetIntProperty(dynamicproperties.WorkerPersistenceGlobalMaxQPS),
		PersistenceMaxQPS:                   dc.GetIntProperty(dynamicproperties.WorkerPersistenceMaxQPS),
		DomainReplicationMaxRetryDuration:   dc.GetDurationProperty(dynamicproperties.WorkerReplicationTaskMaxRetryDuration),
		EnableAsyncWorkflowConsumption:      dc.GetBoolProperty(dynamicproperties.EnableAsyncWorkflowConsumption),
		HostName:                            params.HostName,
	}
	advancedVisWritingMode := dc.GetStringProperty(
		dynamicproperties.WriteVisibilityStoreName,
	)

	if shouldStartIndexer(params, advancedVisWritingMode) {
		config.IndexerCfg = &indexer.Config{
			IndexerConcurrency:             dc.GetIntProperty(dynamicproperties.WorkerIndexerConcurrency),
			ESProcessorNumOfWorkers:        dc.GetIntProperty(dynamicproperties.WorkerESProcessorNumOfWorkers),
			ESProcessorBulkActions:         dc.GetIntProperty(dynamicproperties.WorkerESProcessorBulkActions),
			ESProcessorBulkSize:            dc.GetIntProperty(dynamicproperties.WorkerESProcessorBulkSize),
			ESProcessorFlushInterval:       dc.GetDurationProperty(dynamicproperties.WorkerESProcessorFlushInterval),
			ValidSearchAttributes:          dc.GetMapProperty(dynamicproperties.ValidSearchAttributes),
			EnableQueryAttributeValidation: dc.GetBoolProperty(dynamicproperties.EnableQueryAttributeValidation),
		}
	}

	return config
}

// Start is called to start the service
func (s *Service) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	logger := s.GetLogger()
	logger.Info("worker starting", tag.ComponentWorker)

	s.Resource.Start()
	s.Resource.GetDomainReplicationQueue().Start()

	s.ensureDomainExists(constants.SystemLocalDomainName)
	s.startScanner()
	s.startFixerWorkflowWorker()
	if s.config.IndexerCfg != nil {
		if shouldStartMigrationIndexer(s.params) {
			s.startMigrationDualIndexer()
		} else {
			s.startIndexer()
		}
	}

	s.startReplicator()
	s.startDiagnostics()
	s.startDomainDeprecation()

	if s.GetArchivalMetadata().GetHistoryConfig().ClusterConfiguredForArchival() {
		s.startArchiver()
	}
	if s.config.EnableBatcher() {
		s.ensureDomainExists(constants.BatcherLocalDomainName)
		s.startBatcher()
	}
	if s.config.EnableParentClosePolicyWorker() {
		s.startParentClosePolicyProcessor()
	}
	if s.config.EnableESAnalyzer() {
		s.startESAnalyzer()
	}
	if s.config.EnableFailoverManager() {
		s.startFailoverManager()
	}

	cm := s.startAsyncWorkflowConsumerManager()
	defer cm.Stop()

	logger.Info("worker started", tag.ComponentWorker)
	<-s.stopC
}

// Stop is called to stop the service
func (s *Service) Stop() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	s.GetLogger().Info("worker stopping", tag.ComponentWorker)

	close(s.stopC)

	s.Resource.Stop()
	s.Resource.GetDomainReplicationQueue().Stop()

	s.GetLogger().Info("worker stopped", tag.ComponentWorker)
}

func (s *Service) startParentClosePolicyProcessor() {
	params := &parentclosepolicy.BootstrapParams{
		ServiceClient: s.params.PublicClient,
		MetricsClient: s.GetMetricsClient(),
		Logger:        s.GetLogger(),
		TallyScope:    s.params.MetricScope,
		ClientBean:    s.GetClientBean(),
		DomainCache:   s.GetDomainCache(),
		NumWorkflows:  s.config.NumParentClosePolicySystemWorkflows(),
	}
	processor := parentclosepolicy.New(params)
	if err := processor.Start(); err != nil {
		s.GetLogger().Fatal("error starting parentclosepolicy processor", tag.Error(err))
	}
}

func (s *Service) startESAnalyzer() {
	esClient := s.params.ESClient
	esConfig := s.params.ESConfig
	// when es client is not configured, use os client, this can happen during os migration, we will use os client to read from os
	// there is another case is pinot migration and migration mode is off, in this case nil es config/client is expected
	if esClient == nil && s.params.OSClient != nil {
		esClient = s.params.OSClient
		esConfig = s.params.OSConfig
	}

	analyzer := esanalyzer.New(
		s.params.PublicClient,
		s.GetFrontendClient(),
		s.GetClientBean(),
		esClient,
		s.params.PinotClient,
		esConfig,
		s.params.PinotConfig,
		s.GetLogger(),
		s.params.MetricScope,
		s.Resource,
		s.GetDomainCache(),
		s.config.ESAnalyzerCfg,
	)

	if err := analyzer.Start(); err != nil {
		s.GetLogger().Fatal("error starting esanalyzer", tag.Error(err))
	}
}

func (s *Service) startBatcher() {
	params := &batcher.BootstrapParams{
		Config:        *s.config.BatcherCfg,
		ServiceClient: s.params.PublicClient,
		MetricsClient: s.GetMetricsClient(),
		Logger:        s.GetLogger(),
		TallyScope:    s.params.MetricScope,
		ClientBean:    s.GetClientBean(),
	}
	if err := batcher.New(params).Start(); err != nil {
		s.GetLogger().Fatal("error starting batcher", tag.Error(err))
	}
}

func (s *Service) startScanner() {
	params := &scanner.BootstrapParams{
		Config:     *s.config.ScannerCfg,
		TallyScope: s.params.MetricScope,
	}
	if err := scanner.New(s.Resource, params).Start(); err != nil {
		s.GetLogger().Fatal("error starting scanner", tag.Error(err))
	}
}

func (s *Service) startFixerWorkflowWorker() {
	params := &scanner.BootstrapParams{
		Config:     *s.config.ScannerCfg,
		TallyScope: s.params.MetricScope,
	}
	if err := scanner.NewDataCorruptionWorkflowWorker(s.Resource, params).StartDataCorruptionWorkflowWorker(); err != nil {
		s.GetLogger().Fatal("error starting fixer workflow worker", tag.Error(err))
	}
}

func (s *Service) startDiagnostics() {
	params := diagnostics.Params{
		ServiceClient:   s.params.PublicClient,
		MetricsClient:   s.GetMetricsClient(),
		MessagingClient: s.GetMessagingClient(),
		TallyScope:      s.params.MetricScope,
		ClientBean:      s.GetClientBean(),
		Logger:          s.GetLogger(),
		Invariants:      s.params.DiagnosticsInvariants,
		ClusterMetadata: s.GetClusterMetadata(),
	}
	if err := diagnostics.New(params).Start(); err != nil {
		s.Stop()
		s.GetLogger().Fatal("error starting diagnostics", tag.Error(err))
	}
}

func (s *Service) startReplicator() {
	domainReplicationTaskExecutor := domain.NewReplicationTaskExecutor(
		s.Resource.GetDomainManager(),
		s.Resource.GetTimeSource(),
		s.Resource.GetLogger(),
	)
	msgReplicator := replicator.NewReplicator(
		s.GetClusterMetadata(),
		s.GetClientBean(),
		s.GetLogger(),
		s.GetMetricsClient(),
		s.GetHostInfo(),
		s.GetMembershipResolver(),
		s.GetDomainReplicationQueue(),
		domainReplicationTaskExecutor,
		s.config.DomainReplicationMaxRetryDuration(),
	)
	if err := msgReplicator.Start(); err != nil {
		msgReplicator.Stop()
		s.GetLogger().Fatal("fail to start replicator", tag.Error(err))
	}
}

func (s *Service) startIndexer() {
	visibilityIndexer := indexer.NewIndexer(
		s.config.IndexerCfg,
		s.GetMessagingClient(),
		s.params.ESClient,
		s.params.ESConfig.Indices[constants.VisibilityAppName],
		s.params.ESConfig.ConsumerName,
		s.GetLogger(),
		s.GetMetricsClient(),
	)
	if err := visibilityIndexer.Start(); err != nil {
		visibilityIndexer.Stop()
		s.GetLogger().Fatal("fail to start indexer", tag.Error(err))
	}
}

func (s *Service) startMigrationDualIndexer() {
	visibilityDualIndexer := indexer.NewMigrationDualIndexer(
		s.config.IndexerCfg,
		s.GetMessagingClient(),
		s.params.ESClient,
		s.params.OSClient,
		s.params.ESConfig.Indices[constants.VisibilityAppName],
		s.params.OSConfig.Indices[constants.VisibilityAppName],
		s.params.ESConfig.ConsumerName,
		s.params.OSConfig.ConsumerName,
		s.GetLogger(),
		s.GetMetricsClient(),
	)
	if err := visibilityDualIndexer.Start(); err != nil {
		// not need to call visibilityDualIndexer.Stop() since it has been called inside Start()
		s.GetLogger().Fatal("fail to start indexer", tag.Error(err))
	}
}

func (s *Service) startArchiver() {
	bc := &archiver.BootstrapContainer{
		PublicClient:     s.GetSDKClient(),
		MetricsClient:    s.GetMetricsClient(),
		Logger:           s.GetLogger(),
		HistoryV2Manager: s.GetHistoryManager(),
		DomainCache:      s.GetDomainCache(),
		Config:           s.config.ArchiverConfig,
		ArchiverProvider: s.GetArchiverProvider(),
	}
	clientWorker := archiver.NewClientWorker(bc)
	if err := clientWorker.Start(); err != nil {
		clientWorker.Stop()
		s.GetLogger().Fatal("failed to start archiver", tag.Error(err))
	}
}

func (s *Service) startFailoverManager() {
	params := &failovermanager.BootstrapParams{
		Config:        *s.config.failoverManagerCfg,
		ServiceClient: s.params.PublicClient,
		MetricsClient: s.GetMetricsClient(),
		Logger:        s.GetLogger(),
		TallyScope:    s.params.MetricScope,
		ClientBean:    s.GetClientBean(),
	}
	if err := failovermanager.New(params).Start(); err != nil {
		s.Stop()
		s.GetLogger().Fatal("error starting failoverManager", tag.Error(err))
	}
}

func (s *Service) startAsyncWorkflowConsumerManager() common.Daemon {
	cm := asyncworkflow.NewConsumerManager(
		s.GetLogger(),
		s.GetMetricsClient(),
		s.GetDomainCache(),
		s.Resource.GetAsyncWorkflowQueueProvider(),
		s.GetFrontendClient(),
		asyncworkflow.WithEnabledPropertyFn(s.config.EnableAsyncWorkflowConsumption),
	)
	cm.Start()
	return cm
}

func (s *Service) startDomainDeprecation() {
	params := domaindeprecation.Params{
		Config: domaindeprecation.Config{
			AdminOperationToken: s.config.AdminOperationToken,
		},
		ServiceClient: s.params.PublicClient,
		ClientBean:    s.GetClientBean(),
		Tally:         s.params.MetricScope,
		Logger:        s.GetLogger(),
	}

	if err := domaindeprecation.New(params).Start(); err != nil {
		s.Stop()
		s.GetLogger().Fatal("error starting domain deprecator", tag.Error(err))
	}
}

func (s *Service) ensureDomainExists(domain string) {
	_, err := s.GetDomainManager().GetDomain(context.Background(), &persistence.GetDomainRequest{Name: domain})
	switch err.(type) {
	case nil:
		// noop
	case *types.EntityNotExistsError:
		s.GetLogger().Info(fmt.Sprintf("domain %s does not exist, attempting to register domain", domain))
		s.registerSystemDomain(domain)
	default:
		s.GetLogger().Fatal("failed to verify if system domain exists", tag.Error(err))
	}
}

func (s *Service) registerSystemDomain(domain string) {

	currentClusterName := s.GetClusterMetadata().GetCurrentClusterName()
	_, err := s.GetDomainManager().CreateDomain(context.Background(), &persistence.CreateDomainRequest{
		Info: &persistence.DomainInfo{
			ID:          getDomainID(domain),
			Name:        domain,
			Description: "Cadence internal system domain",
		},
		Config: &persistence.DomainConfig{
			Retention:  constants.SystemDomainRetentionDays,
			EmitMetric: true,
		},
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: currentClusterName,
			Clusters:          cluster.GetOrUseDefaultClusters(currentClusterName, nil),
		},
		IsGlobalDomain:  false,
		FailoverVersion: constants.EmptyVersion,
	})
	if err != nil {
		if _, ok := err.(*types.DomainAlreadyExistsError); ok {
			return
		}
		s.GetLogger().Fatal("failed to register system domain", tag.Error(err))
	}
}

func getDomainID(domain string) string {
	var domainID string
	switch domain {
	case constants.SystemLocalDomainName:
		domainID = constants.SystemDomainID
	case constants.BatcherLocalDomainName:
		domainID = constants.BatcherDomainID
	case constants.ShadowerLocalDomainName:
		domainID = constants.ShadowerDomainID
	}
	return domainID
}

func shouldStartIndexer(params *resource.Params, advancedWritingMode dynamicproperties.StringPropertyFn) bool {
	// only start indexer when advanced visibility writing mode is set to on and advanced visibility store is configured
	if !common.IsAdvancedVisibilityWritingEnabled(advancedWritingMode(), params.PersistenceConfig.IsAdvancedVisibilityConfigExist()) {
		return false
	}

	// when it is using pinot and not in migration mode, indexer should not be started since Pinot will direclty ingest from kafka
	if params.PersistenceConfig.AdvancedVisibilityStore == constants.PinotVisibilityStoreName &&
		params.PinotConfig != nil &&
		!params.PinotConfig.Migration.Enabled {
		return false
	}

	return true
}

func shouldStartMigrationIndexer(params *resource.Params) bool {
	// not need to check IsAdvancedVisibilityWritingEnabled here since it was already checked or the s.config.IndexerCfg will be nil
	// when it is using OS and in migration mode, we should start dual indexer to write to both ES and OS
	if params.PersistenceConfig.AdvancedVisibilityStore == constants.OSVisibilityStoreName &&
		params.OSConfig != nil &&
		params.OSConfig.Migration.Enabled {
		return true
	}

	return false
}
