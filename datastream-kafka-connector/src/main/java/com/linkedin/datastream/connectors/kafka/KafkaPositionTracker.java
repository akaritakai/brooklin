/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractScheduledService;

import com.linkedin.datastream.common.diag.KafkaPositionKey;
import com.linkedin.datastream.common.diag.KafkaPositionValue;
import com.linkedin.datastream.server.DatastreamTask;

/**
 * KafkaPositionTracker is intended to be used with a Kafka-based Connector task to keep track of the current
 * offset/position of the Connector task's consumer.
 *
 * The information stored can then be queried via the /diag endpoint for diagnostic and analytic purposes.
 */
public class KafkaPositionTracker implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaPositionTracker.class);

  /**
   * The suffix for Kafka consumer metrics that record records lag.
   */
  private static final String RECORDS_LAG_METRIC_NAME_SUFFIX = "records-lag";

  /**
   * The position data for this DatastreamTask.
   */
  private final Map<KafkaPositionKey, KafkaPositionValue> _positions = new ConcurrentHashMap<>();

  /**
   * A map of TopicPartitions to KafkaPositionKeys currently owned/operated on by this KafkaPositionTracker instance.
   */
  private final Map<TopicPartition, KafkaPositionKey> _ownedKeys = new ConcurrentHashMap<>();

  /**
   * A set of TopicPartitions which are assigned to us, but for which we have not yet received any records or
   * consumer position data for.
   */
  private final Set<TopicPartition> _uninitializedPartitions = ConcurrentHashMap.newKeySet();

  /**
   * A look-up table of TopicPartition -> MetricName as they are encountered to speed consumer metric look up.
   */
  private final Map<TopicPartition, MetricName> _metricNameCache = new HashMap<>();

  /**
   * True if this position tracker has been closed. False, otherwise.
   */
  private final AtomicBoolean _closed = new AtomicBoolean(false);

  /**
   * The task prefix for the DatastreamTask.
   * @see com.linkedin.datastream.server.DatastreamTask#getTaskPrefix()
   */
  private final String _datastreamTaskPrefix;

  /**
   * The unique DatastreamTask name.
   * @see com.linkedin.datastream.server.DatastreamTask#getDatastreamTaskName()
   */
  private final String _datastreamTaskName;

  /**
   * The time at which the Connector task was instantiated.
   */
  private final Instant _connectorTaskStartTime;

  /**
   * A supplier that provides a Kafka consumer which can be given to {@link BrokerOffsetFetcher}.
   */
  private final Supplier<Consumer<?, ?>> _consumerSupplier;

  /**
   * The number of offsets to fetch from the broker per endOffsets() RPC call. This should be chosen to avoid timeouts
   * (larger requests are more likely to cause timeouts).
   */
  private final int _brokerOffsetsFetchSize;

  /**
   * The service responsible for periodically fetching offsets from the broker.
   */
  private final BrokerOffsetFetcher _brokerOffsetFetcher;

  /**
   * The client id of the Kafka consumer used by the Connector task. Used to fetch metrics.
   */
  private String _clientId;

  /**
   * The metrics format supported by the Kafka consumer used by the Connector task.
   */
  private ConsumerMetricsFormatSupport _consumerMetricsSupport;

  /**
   * Constructor for a KafkaPositionTracker.
   *
   * @param datastreamTaskPrefix The task prefix for the DatastreamTask
   *                             {@see com.linkedin.datastream.server.DatastreamTask#getTaskPrefix()}
   * @param datastreamTaskName The DatastreamTask name
   *                           {@see com.linkedin.datastream.server.DatastreamTask#getDatastreamTaskName()}
   * @param connectorTaskStartTime The time at which the associated DatastreamTask started
   * @param consumerSupplier A Consumer supplier that is suitable for querying the brokers that the Connector task is
   *                         talking to
   * @param positionTrackerConfig User-supplied configuration settings
   */
  private KafkaPositionTracker(String datastreamTaskPrefix, String datastreamTaskName, Instant connectorTaskStartTime,
                                Supplier<Consumer<?, ?>> consumerSupplier,
                                KafkaPositionTrackerConfig positionTrackerConfig) {
    _datastreamTaskPrefix = datastreamTaskPrefix;
    _datastreamTaskName = datastreamTaskName;
    _connectorTaskStartTime = connectorTaskStartTime;
    _consumerSupplier = consumerSupplier;
    _brokerOffsetsFetchSize = positionTrackerConfig.getBrokerOffsetsFetchSize();
    if (positionTrackerConfig.isEnableBrokerOffsetFetcher()) {
      _brokerOffsetFetcher = new BrokerOffsetFetcher(datastreamTaskName, this, positionTrackerConfig);
      _brokerOffsetFetcher.startAsync();
    } else {
      _brokerOffsetFetcher = null;
    }
  }


  /**
   * Initializes position data for the assigned partitions. This method should be called whenever the Connector's
   * consumer finishes assigning partitions.
   *
   * @see AbstractKafkaBasedConnectorTask#onPartitionsAssigned(Collection) for how this method is used in a connector
   *      task
   * @param topicPartitions the topic partitions which have been assigned
   */
  public synchronized void onPartitionsAssigned(Collection<TopicPartition> topicPartitions) {
    Instant assignmentTime = Instant.now();
    topicPartitions.forEach(topicPartition -> {
      KafkaPositionKey key = new KafkaPositionKey(topicPartition.topic(), topicPartition.partition(),
          _datastreamTaskPrefix, _datastreamTaskName, _connectorTaskStartTime);
      _ownedKeys.put(topicPartition, key);
      _uninitializedPartitions.add(topicPartition);
      KafkaPositionValue value = _positions.computeIfAbsent(key, s -> new KafkaPositionValue());
      value.setAssignmentTime(assignmentTime);
    });
  }

  /**
   * Frees position data for partitions which have been unassigned. This method should be called whenever the
   * Connector's consumer is about to rebalance (and thus unassign partitions).
   *
   * @see AbstractKafkaBasedConnectorTask#onPartitionsRevoked(Collection) for how this method is used in a connector
   *      task
   * @param topicPartitions the topic partitions which were previously assigned
   */
  public synchronized void onPartitionsRevoked(Collection<TopicPartition> topicPartitions) {
    topicPartitions.forEach(topicPartition -> {
      _metricNameCache.remove(topicPartition);
      _uninitializedPartitions.remove(topicPartition);
      Optional.ofNullable(_ownedKeys.remove(topicPartition)).ifPresent(_positions::remove);
    });
  }

  /**
   * Updates the position data after the Connector's consumer has finished polling, using both the returned records and
   * the available internal Kafka consumer metrics.
   *
   * This method will only update position data for partitions which have received records.
   *
   * @param records the records fetched from {@link Consumer#poll(Duration)}
   * @param metrics the metrics for the Kafka consumer as fetched from {@link Consumer#metrics()}
   */
  public synchronized void onRecordsReceived(ConsumerRecords<?, ?> records, Map<MetricName, ? extends Metric> metrics) {
    Instant receivedTime = Instant.now();
    records.partitions().forEach(topicPartition -> {
      // Assumption: onPartitionsAssigned() called at least once for this partition
      // Assumption: onPartitionsRevoked() not called since last onPartitionsAssigned() for this partition
      KafkaPositionKey key = _ownedKeys.get(topicPartition);
      KafkaPositionValue value = _positions.get(key);

      // Assumption: Last message in partitionRecords has the largest offset
      List<? extends ConsumerRecord<?, ?>> partitionRecords = records.records(topicPartition);
      ConsumerRecord<?, ?> record = partitionRecords.get(partitionRecords.size() - 1);
      value.setLastNonEmptyPollTime(receivedTime);
      // Why add +1? The consumer's position is the offset of the next record it expects.
      value.setConsumerOffset(record.offset() + 1);
      value.setLastRecordReceivedTimestamp(Instant.ofEpochMilli(record.timestamp()));

      Long consumerLag = getConsumerLagFromMetric(metrics, topicPartition);
      if (consumerLag != null) {
        long brokerOffset = (record.offset() + 1) + consumerLag;
        value.setBrokerOffset(brokerOffset);
        value.setLastBrokerQueriedTime(receivedTime);
      }
    });
  }

  /**
   * Returns a Set of TopicPartitions which are assigned to us, but for which we have not yet received any records or
   * consumer position data for.
   *
   * @return the Set of TopicPartitions which are not yet initialized
   */
  public synchronized Set<TopicPartition> getUninitializedPartitions() {
    return Collections.unmodifiableSet(_uninitializedPartitions);
  }

  /**
   * Fills the current consumer offset into the position data for the given TopicPartition, causing the TopicPartition
   * to no longer need initialization.
   *
   * This offset is typically found by the Connector's consumer by calling {@link Consumer#position(TopicPartition)}
   * after a successful {@link Consumer#poll(Duration)}.
   *
   * @param topicPartition The given TopicPartition to provide position data for
   * @param consumerOffset The Connector consumer's offset for this topic partition as if specified by
   *                       {@link Consumer#position(TopicPartition)}
   */
  public synchronized void initializePartition(TopicPartition topicPartition, Long consumerOffset) {
    // Assumption: onPartitionsAssigned() called at least once for this partition
    // Assumption: onPartitionsRevoked() not called since last onPartitionsAssigned() for this partition
    KafkaPositionKey key = _ownedKeys.get(topicPartition);
    KafkaPositionValue value = _positions.get(key);
    value.setConsumerOffset(consumerOffset);
    _uninitializedPartitions.remove(topicPartition);
  }

  /**
   * Returns the position data stored in this instance.
   *
   * @return the position data stored in this instance
   */
  public Map<KafkaPositionKey, KafkaPositionValue> getPositions() {
    return Collections.unmodifiableMap(_positions);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    _closed.set(true);
    if (_brokerOffsetFetcher != null) {
      _brokerOffsetFetcher.close();
    }
  }

  /**
   * Checks the Kafka consumer metrics as acquired by {@link Consumer#metrics()} to see if it contains information on
   * record lag (the lag between the consumer and the broker) for a given TopicPartition.
   *
   * If it does, the lag value is returned.
   *
   * @param metrics The metrics returned by the Kafka consumer to check
   * @param topicPartition The TopicPartition to match against
   * @return the lag value if it can be found
   */
  private Long getConsumerLagFromMetric(Map<MetricName, ? extends Metric> metrics, TopicPartition topicPartition) {
    return Optional.ofNullable(tryCreateMetricName(topicPartition, metrics.keySet()))
        .map(name -> (Metric) metrics.get(name))
        .map(Metric::metricValue)
        .filter(value -> value instanceof Double)
        .map(value -> ((Double) value).longValue())
        .orElse(null);
  }

  /**
   * Attempts to return the metric name containing record lag information if it exists. This method will attempt to
   * return the cached value before calculating it (calculating the value is expensive).
   *
   * @param topicPartition the provided topic partition
   * @param metricNames the collection of metric names
   * @return the metric name containing record lag information, if it can be derived
   */
  private MetricName tryCreateMetricName(TopicPartition topicPartition, Collection<MetricName> metricNames) {
    // Fail fast if the consumer does not emit record lag to metrics.
    if (_consumerMetricsSupport == ConsumerMetricsFormatSupport.NONE) {
      return null;
    }

    // If we haven't established consumer support for emitting record lag to metrics, do so.
    if (_clientId == null || _consumerMetricsSupport == null) {
      establishConsumerMetricsSupportLevel(metricNames);
      return tryCreateMetricName(topicPartition, metricNames);
    }

    return _metricNameCache.computeIfAbsent(topicPartition, s -> {
      switch (_consumerMetricsSupport) {
        case KIP_92:
          return new MetricName(topicPartition + "." + RECORDS_LAG_METRIC_NAME_SUFFIX,
              "consumer-fetch-manager-metrics", "", ImmutableMap.of("client-id", _clientId));
        case KIP_225:
          return new MetricName(RECORDS_LAG_METRIC_NAME_SUFFIX, "consumer-fetch-manager-metrics", "",
              ImmutableMap.of("client-id", _clientId, "topic", topicPartition.topic(),
                  "partition", String.valueOf(topicPartition.partition())));
        default: return null;
      }
    });
  }

  private void establishConsumerMetricsSupportLevel(Collection<MetricName> metricNames) {
    Optional<MetricName> metricName = metricNames.stream()
        .filter(name -> name.name() != null)
        .filter(name -> name.tags() != null)
        .filter(name -> name.name().endsWith(RECORDS_LAG_METRIC_NAME_SUFFIX))
        .findAny();
    if (metricName.isPresent()) {
      Optional<String> clientId = Optional.ofNullable(metricName.get().tags())
          .map(tags -> tags.get("client-id"));
      if (clientId.isPresent()) {
        _clientId = clientId.get();
        _consumerMetricsSupport = metricName.get().name().length() == RECORDS_LAG_METRIC_NAME_SUFFIX.length()
            ? ConsumerMetricsFormatSupport.KIP_225 : ConsumerMetricsFormatSupport.KIP_92;
        return;
      }
    }
    _consumerMetricsSupport = ConsumerMetricsFormatSupport.NONE;
  }

  /**
   * Returns a Builder which can be used to construct a {@link KafkaPositionTracker}.
   *
   * @return a Builder for this class
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Uses the specified consumer to make RPC calls of {@link Consumer#endOffsets(Collection)} to get the broker's latest
   * offsets for the specified TopicPartitions, and then updates the position data. The partitions will be fetched in
   * batches to reduce the likelihood of a given call timing out.
   *
   * Note that the provided consumer must not be operated on by any other thread, or a concurrent modification condition
   * may arise.
   *
   * Ideally the partitions provided to this method are owned all by the same broker for performance purposes.
   *
   * Use externally for testing purposes only.
   */
  @VisibleForTesting
  void queryBrokerForLatestOffsets(Consumer<?, ?> consumer, Set<TopicPartition> partitions, Duration requestTimeout) {
    for (List<TopicPartition> batch : Iterables.partition(partitions, _brokerOffsetsFetchSize)) {
      LOG.trace("Fetching the latest offsets for partitions: {}", batch);
      Instant queryTime = Instant.now();
      try {
        consumer.endOffsets(batch, requestTimeout).forEach((topicPartition, offset) -> {
          if (offset != null) {
            // Race condition could exist where we might be unassigned the topic in a different thread while we are in
            // this thread, so do not create/initialize the key/value in the map.
            final KafkaPositionKey key = _ownedKeys.get(topicPartition);
            if (key != null) {
              final KafkaPositionValue value = _positions.get(key);
              if (value != null) {
                value.setLastBrokerQueriedTime(queryTime);
                value.setBrokerOffset(offset);
              }
            }
          }
        });
      } catch (Exception e) {
        LOG.trace("Unable to fetch latest offsets for partitions: {}", batch, e);
      } finally {
        LOG.trace("Finished fetching the latest offsets for partitions {} in {} ms", batch,
            Duration.between(queryTime, Instant.now()).toMillis());
      }
    }
  }

  /**
   * Supplies a consumer usable for fetching broker offsets.
   *
   * @return a consumer usable for RPC calls
   */
  @VisibleForTesting
  Supplier<Consumer<?, ?>> getConsumerSupplier() {
    return _consumerSupplier;
  }

  /**
   * Describes the metrics format supported by a Kafka consumer.
   */
  private enum ConsumerMetricsFormatSupport {
    /**
     * The Kafka consumer does not expose record lag.
     */
    NONE,

    /**
     * The Kafka consumer exposes record lag by KIP-92, which applies to Kafka versions >= 0.10.2.0 and < 1.1.0.
     * @see <a href="https://cwiki.apache.org/confluence/x/bhX8Awr">KIP-92</a>
     */
    KIP_92,

    /**
     * The Kafka consumer exposes record lag by KIP-225 (superseding KIP-92), which applies to Kafka versions >= 1.1.0.
     * @see <a href="https://cwiki.apache.org/confluence/x/uaBzB">KIP-225</a>
     */
    KIP_225
  }

  /**
   * Implements a periodic service which queries the broker for its latest partition offsets.
   */
  private static class BrokerOffsetFetcher extends AbstractScheduledService implements Closeable {
    /**
     * Counts the number of instantiations of this class (useful for uniquely identifying an instance).
     */
    private static final AtomicLong INSTANTIATION_COUNTER = new AtomicLong(0L);

    /**
     * The frequency at which to fetch offsets from the broker using the endOffsets() RPC call.
     */
    private final Duration _brokerOffsetsFetchInterval;

    /**
     * The maximum duration that a consumer request is allowed to take before it times out.
     */
    private final Duration _consumerRequestTimeout;

    /**
     * The KafkaPositionTracker object which created us.
     */
    private final KafkaPositionTracker _kafkaPositionTracker;

    /**
     * True if we should calculate performance leadership to improve the broker query performance, false otherwise.
     */
    private final boolean _enablePartitionLeadershipCalculation;

    /**
     * The frequency at which we should fetch partition leadership.
     */
    private final Duration _partitionLeadershipCalculationFrequency;

    /**
     * A cache of partition leadership used to optimize the endOffset() RPC calls made. This is necessary as an
     * endOffset() RPC call can in a very large fanout of requests (as it may need to talk to a lot of brokers). By
     * keeping a very weakly consistent list of who owns what partitions, we can offer a performance boost. If this map
     * is very out-of-date, it is unlikely to be much worse than not using it at all.
     */
    private final Map<TopicPartition, Node> _partitionLeadershipMap = new ConcurrentHashMap<>();

    /**
     * The service name (used to identify this thread in a thread dump or heap dump).
     */
    private final String _serviceName;

    /**
     * The underlying Consumer used to make the endOffsets() RPC call.
     */
    private Consumer<?, ?> _consumer;

    /**
     * The time of the last partition leadership calculation.
     */
    private Instant _lastPartitionLeadershipCalculation;

    /**
     * The time that this service was started.
     */
    private Instant _startTime; // Defined to help investigation issues (when you have a heap dump or are in a debugger)

    /**
     * The time that this service was stopped.
     */
    private Instant _stopTime; // Defined to help investigation issues (when you have a heap dump or are in a debugger)

    /**
     * Constructor for this class.
     *
     * @param brooklinTaskId The DatastreamTask name
     *                       {@see com.linkedin.datastream.server.DatastreamTask#getDatastreamTaskName()}
     * @param kafkaPositionTracker The KafkaPositionTracker instantiating this object
     * @param positionTrackerConfig User-supplied configuration settings
     */
    public BrokerOffsetFetcher(String brooklinTaskId, KafkaPositionTracker kafkaPositionTracker,
                               KafkaPositionTrackerConfig positionTrackerConfig) {
      _serviceName = "kafkaPositionTracker-" + brooklinTaskId + "-" + INSTANTIATION_COUNTER.incrementAndGet();
      _kafkaPositionTracker = kafkaPositionTracker;
      _enablePartitionLeadershipCalculation = positionTrackerConfig.isEnablePartitionLeadershipCalculation();
      _brokerOffsetsFetchInterval = positionTrackerConfig.getBrokerOffsetFetcherInterval();
      _consumerRequestTimeout = positionTrackerConfig.getBrokerRequestTimeout();
      _partitionLeadershipCalculationFrequency = positionTrackerConfig.getPartitionLeadershipCalculationFrequency();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void startUp() {
      _startTime = Instant.now();
      _consumer = _kafkaPositionTracker._consumerSupplier.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void runOneIteration() {
      // Find which partitions have stale broker offset information
      Instant staleBy = Instant.now().minus(_brokerOffsetsFetchInterval);
      Set<TopicPartition> partitionsNeedingUpdate = _kafkaPositionTracker._ownedKeys
          .entrySet()
          .stream()
          .filter(e -> {
            // Race condition could exist where we might be unassigned the topic in a different thread while we are in
            // this thread.
            KafkaPositionValue value = _kafkaPositionTracker._positions.get(e.getValue());
            Instant lastQueryTime = Optional.ofNullable(value).map(KafkaPositionValue::getLastBrokerQueriedTime)
                .orElse(null);
            return value != null && (lastQueryTime == null || value.getLastBrokerQueriedTime().isBefore(staleBy));
          })
          .map(Map.Entry::getKey)
          .collect(Collectors.toSet());

      try {
        if (_enablePartitionLeadershipCalculation) {
          Instant calculationStaleBy = Optional.ofNullable(_lastPartitionLeadershipCalculation)
              .map(lastCalculation -> lastCalculation.plus(_partitionLeadershipCalculationFrequency))
              .orElse(Instant.MIN);
          if (Instant.now().isAfter(calculationStaleBy)) {
            updatePartitionLeadershipMap();
            _lastPartitionLeadershipCalculation = Instant.now();
          }
        }
      } catch (Exception e) {
        LOG.debug("Failed to update the partition leadership map. Using stale leadership data. Reason: {}",
            e.getMessage());
      }

      // Query the broker for its offsets for those partitions
      batchPartitionsByLeader(partitionsNeedingUpdate).forEach(partitionLeaderBatch -> {
        try {
          if (!_kafkaPositionTracker._closed.get()) {
            _kafkaPositionTracker.queryBrokerForLatestOffsets(_consumer, partitionLeaderBatch, _consumerRequestTimeout);
          }
        } catch (Exception e) {
          LOG.debug("Failed to query latest broker offsets for this leader batch via endOffsets() RPC. Reason: {}",
              e.getMessage());
        }
      });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void shutDown() {
      _stopTime = Instant.now();
      if (_consumer != null) {
        try {
          _consumer.close();
        } catch (Exception ignored) {
        }
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Scheduler scheduler() {
      return Scheduler.newFixedRateSchedule(0, _brokerOffsetsFetchInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()  {
      stopAsync();
      try {
        _consumer.wakeup();
      } catch (Exception ignored) {
      }
    }

    /**
     * Queries a Kafka broker for the leader of each partition and caches that information.
     */
    private void updatePartitionLeadershipMap() {
      if (!_kafkaPositionTracker._closed.get()) {
        LOG.debug("Updating partition leadership map");
        Optional.ofNullable(_consumer.listTopics(_consumerRequestTimeout)).ifPresent(response -> {
          Map<TopicPartition, Node> updateMap = response.values().stream()
              .filter(Objects::nonNull)
              .flatMap(Collection::stream)
              .filter(partitionInfo -> partitionInfo != null && partitionInfo.topic() != null && partitionInfo.leader() != null)
              .collect(Collectors.toMap(
                  partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()),
                  PartitionInfo::leader));
          _partitionLeadershipMap.keySet().retainAll(updateMap.keySet());
          _partitionLeadershipMap.putAll(updateMap);
        });
      }
    }

    /**
     * Groups topic partitions into batches based on their leader.
     * @param topicPartitions the topic partitions to group by leader
     * @return a list of topic partitions batches
     */
    private List<Set<TopicPartition>> batchPartitionsByLeader(Set<TopicPartition> topicPartitions) {
      if (!_enablePartitionLeadershipCalculation || _partitionLeadershipMap.isEmpty()) {
        if (_partitionLeadershipMap.isEmpty()) {
          LOG.debug("Leadership unknown for all topic partitions");
        }
        return Collections.singletonList(topicPartitions);
      }

      Map<Node, Set<TopicPartition>> assignedPartitions = new HashMap<>();
      Set<TopicPartition> unassignedPartitions = new HashSet<>();

      topicPartitions.forEach(topicPartition -> {
        Node leader = _partitionLeadershipMap.get(topicPartition);
        if (leader == null) {
          LOG.debug("Leader unknown for topic partition {}", topicPartition);
          unassignedPartitions.add(topicPartition);
        } else {
          LOG.trace("Leader for topic partition {} is {}", topicPartition, leader);
          assignedPartitions.computeIfAbsent(leader, s -> new HashSet<>()).add(topicPartition);
        }
      });

      final List<Set<TopicPartition>> batches = new ArrayList<>();
      batches.addAll(assignedPartitions.values());
      batches.add(unassignedPartitions);
      return batches;
    }
  }

  /**
   * Builder which can be used to create an instance of {@link KafkaPositionTracker}.
   */
  public static class Builder {
    private Instant _connectorTaskStartTime;
    private Supplier<Consumer<?, ?>> _consumerSupplier;
    private String _datastreamTaskName;
    private String _datastreamTaskPrefix;
    private KafkaPositionTrackerConfig _kafkaPositionTrackerConfig;

    /**
     * Configures this builder with the time at which the associated DatastreamTask was started. This value is required
     * to construct a {@link KafkaPositionTracker} object and must be provided before {@link #build()} is called.
     *
     * @param connectorTaskStartTime the time at which the associated DatastreamTask was started
     * @return a builder configured with the connectorTaskStartTime param
     */
    public Builder withConnectorTaskStartTime(Instant connectorTaskStartTime) {
      _connectorTaskStartTime = connectorTaskStartTime;
      return this;
    }

    /**
     * Configures the builder with a Consumer supplier that is suitable for querying the brokers that the Connector task
     * is talking to. This value is required to construct a {@link KafkaPositionTracker} object and must be provided
     * before {@link #build()} is called.
     *
     * @param consumerSupplier A Consumer supplier that is suitable for querying the brokers that the Connector task is
     *                         talking to
     * @return a builder configured with the consumerSupplier param
     */
    public Builder withConsumerSupplier(Supplier<Consumer<?, ?>> consumerSupplier) {
      _consumerSupplier = consumerSupplier;
      return this;
    }

    /**
     * Configures the builder with the associated {@link DatastreamTask}. This class requires two pieces of data from
     * the DatastreamTask it is associated with, the DatastreamTask's task prefix and task name, which it collects using
     * {@link DatastreamTask#getTaskPrefix()} and {@link DatastreamTask#getDatastreamTaskName()} respectively. These
     * values are required to construct a {@link KafkaPositionTracker} object and must be provided before
     * {@link #build()} is called.
     *
     * @param datastreamTask The DatastreamTask associated with this position tracker
     * @return a builder configured with the datastreamTaskPrefix and datastreamTaskName params acquired from the
     *         provided DatastreamTask
     */
    public Builder withDatastreamTask(DatastreamTask datastreamTask) {
      _datastreamTaskName = datastreamTask.getDatastreamTaskName();
      _datastreamTaskPrefix = datastreamTask.getTaskPrefix();
      return this;
    }

    /**
     * This function is no-op and is preserved for compatibility purposes.
     */
    public Builder withIsConnectorTaskAlive(Supplier<Boolean> isConnectorTaskAlive) {
      return this;
    }

    /**
     * Configures the builder with various user-supplied configuration settings.
     *
     * @param kafkaPositionTrackerConfig user-supplied configuration settings
     * @return a builder configured with the kafkaPositionTrackerConfig param
     */
    public Builder withKafkaPositionTrackerConfig(KafkaPositionTrackerConfig kafkaPositionTrackerConfig) {
      _kafkaPositionTrackerConfig = kafkaPositionTrackerConfig;
      return this;
    }

    /**
     * Uses the information already provided to the current instantiation of the builder to create an instance of the
     * {@link KafkaPositionTracker} class.
     *
     * @return a {@link KafkaPositionTracker} object configured using the instance data from this builder
     */
    public KafkaPositionTracker build() {
      return new KafkaPositionTracker(_datastreamTaskPrefix, _datastreamTaskName, _connectorTaskStartTime,
          _consumerSupplier, _kafkaPositionTrackerConfig);
    }
  }
}
