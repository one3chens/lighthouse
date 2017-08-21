/**
 * @license Copyright 2017 Google Inc. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
'use strict';

const Node = require('../node');
const TcpConnection = require('./tcp-connection');

// see https://cs.chromium.org/search/?q=kDefaultMaxNumDelayableRequestsPerClient&sq=package:chromium&type=cs
const DEFAULT_MAXIMUM_CONCURRENT_REQUESTS = 10;
const DEFAULT_RESPONSE_TIME = 30;
const DEFAULT_RTT = 150;
const DEFAULT_THROUGHPUT = 1600 * 1024; // 1.6 Mbps
const DEFAULT_CPU_MULTIPLIER = 5;

function groupBy(items, keyFunc) {
  const grouped = new Map();
  items.forEach(item => {
    const key = keyFunc(item);
    const group = grouped.get(key) || [];
    group.push(item);
    grouped.set(key, group);
  });

  return grouped;
}

class Estimator {
  /**
   * @param {!Node} graph
   * @param {{rtt: number, throughput: number, defaultResponseTime: number,
   *    maximumConcurrentRequests: number}=} options
   */
  constructor(graph, options) {
    this._graph = graph;
    this._options = Object.assign(
      {
        rtt: DEFAULT_RTT,
        throughput: DEFAULT_THROUGHPUT,
        defaultResponseTime: DEFAULT_RESPONSE_TIME,
        maximumConcurrentRequests: DEFAULT_MAXIMUM_CONCURRENT_REQUESTS,
        cpuMultiplier: DEFAULT_CPU_MULTIPLIER,
      },
      options
    );

    this._rtt = this._options.rtt;
    this._throughput = this._options.throughput;
    this._defaultResponseTime = this._options.defaultResponseTime;
    this._maximumConcurrentRequests = Math.min(
      TcpConnection.maximumSaturatedConnections(this._rtt, this._throughput),
      this._options.maximumConcurrentRequests
    );
    this._cpuMultiplier = this._options.cpuMultiplier;
  }

  /**
   * @param {!WebInspector.NetworkRequest} record
   * @return {number}
   */
  static getResponseTime(record) {
    const timing = record._timing;
    return (timing && timing.receiveHeadersEnd - timing.sendEnd) || Infinity;
  }

  _initializeNetworkRecords() {
    const records = [];

    this._graph.getRootNode().traverse(node => {
      if (node.type === Node.TYPES.NETWORK) {
        records.push(node.record);
      }
    });

    this._networkRecords = records;
    return records;
  }

  _initializeNetworkConnections() {
    const connections = new Map();
    const recordsByConnection = groupBy(this._networkRecords, record => record.connectionId);

    for (const [connectionId, records] of recordsByConnection.entries()) {
      const isSsl = records[0].parsedURL.scheme === 'https';
      let responseTime = records.reduce(
        (min, record) => Math.min(min, Estimator.getResponseTime(record)),
        Infinity
      );

      if (!Number.isFinite(responseTime)) {
        responseTime = this._defaultResponseTime;
      }

      const connection = new TcpConnection(this._rtt, this._throughput, responseTime, isSsl);

      connections.set(connectionId, connection);
    }

    this._connections = connections;
    return connections;
  }

  _initializeAuxiliaryData() {
    this._nodeAuxiliaryData = new Map();
    this._nodesUnprocessed = new Set();
    this._nodesCompleted = new Set();
    this._nodesInProcess = new Set();
    this._nodesInQueue = new Set(); // TODO: replace this with priority queue
    this._connectionsInUse = new Set();
    this._numberInProcessByType = new Map();
  }

  /**
   * @param {string} type
   * @return {number}
   */
  _numberInProcess(type) {
    return this._numberInProcessByType.get(type) || 0;
  }

  /**
   * @param {!Node} node
   * @param {!Object} values
   */
  _setAuxData(node, values) {
    const auxData = this._nodeAuxiliaryData.get(node) || {};
    Object.assign(auxData, values);
    this._nodeAuxiliaryData.set(node, auxData);
  }

  /**
   * @param {!Node} node
   * @param {number} queuedTime
   */
  _markNodeAsInQueue(node, queuedTime) {
    this._nodesInQueue.add(node);
    this._nodesUnprocessed.delete(node);
    this._setAuxData(node, {queuedTime});
  }

  /**
   * @param {!Node} node
   * @param {number} startTime
   */
  _markNodeAsInProcess(node, startTime) {
    this._nodesInQueue.delete(node);
    this._nodesInProcess.add(node);
    this._numberInProcessByType.set(node.type, this._numberInProcess(node.type) + 1);
    this._setAuxData(node, {startTime});
  }

  /**
   * @param {!Node} node
   * @param {number} endTime
   */
  _markNodeAsComplete(node, endTime) {
    this._nodesCompleted.add(node);
    this._nodesInProcess.delete(node);
    this._numberInProcessByType.set(node.type, this._numberInProcess(node.type) - 1);
    this._setAuxData(node, {endTime});

    // Try to add all its dependents to the queue
    for (const dependent of node.getDependents()) {
      // Skip this node if it's already been completed
      if (this._nodesCompleted.has(dependent)) continue;

      // Skip this node if one of its dependencies hasn't finished yet
      const dependencies = dependent.getDependencies();
      if (dependencies.some(dependency => !this._nodesCompleted.has(dependency))) continue;

      // Otherwise add it to the queue
      this._markNodeAsInQueue(dependent, endTime);
    }
  }

  /**
   * @param {!Node} node
   * @param {number} totalElapsedTime
   */
  _startNodeIfPossible(node, totalElapsedTime) {
    if (node.type === Node.TYPES.CPU) {
      // Start a CPU task if there's no other CPU task in process
      if (this._numberInProcess(node.type) === 0) {
        this._markNodeAsInProcess(node, totalElapsedTime);
        this._setAuxData(node, {timeElapsed: 0});
      }

      return;
    }

    if (node.type !== Node.TYPES.NETWORK) throw new Error('Unsupported');

    const connection = this._connections.get(node.record.connectionId);
    const numberOfActiveRequests = this._numberInProcess(node.type);

    // Start a network request if the connection isn't in use and we're not at max requests
    if (
      numberOfActiveRequests >= this._maximumConcurrentRequests ||
      this._connectionsInUse.has(connection)
    ) {
      return;
    }

    this._markNodeAsInProcess(node, totalElapsedTime);
    this._setAuxData(node, {
      timeElapsed: 0,
      timeElapsedOvershoot: 0,
      bytesDownloaded: 0,
    });

    this._connectionsInUse.add(connection);
  }

  _updateNetworkCapacity() {
    for (const connection of this._connectionsInUse) {
      connection.setThroughput(this._throughput / this._nodesInProcess.size);
    }
  }

  /**
   * @param {!Node} node
   * @return {number}
   */
  _estimateTimeRemaining(node) {
    if (node.type === Node.TYPES.CPU) {
      const auxData = this._nodeAuxiliaryData.get(node);
      const totalDuration = Math.round(node.event.dur / 1000 * this._cpuMultiplier);
      const estimatedTimeElapsed = totalDuration - auxData.timeElapsed;
      this._setAuxData(node, {estimatedTimeElapsed});
      return estimatedTimeElapsed;
    }

    if (node.type !== Node.TYPES.NETWORK) throw new Error('Unsupported');

    const auxData = this._nodeAuxiliaryData.get(node);
    const connection = this._connections.get(node.record.connectionId);
    const calculation = connection.calculateTimeToDownload(
      node.record.transferSize - auxData.bytesDownloaded,
      auxData.timeElapsed
    );

    const estimatedTimeElapsed = calculation.timeElapsed + auxData.timeElapsedOvershoot;
    this._setAuxData(node, {estimatedTimeElapsed});
    return estimatedTimeElapsed;
  }

  /**
   * @return {number}
   */
  _findNextNodeCompletionTime() {
    let minimumTime = Infinity;
    for (const node of this._nodesInProcess) {
      minimumTime = Math.min(minimumTime, this._estimateTimeRemaining(node));
    }

    return minimumTime;
  }

  /**
   * @param {!Node} node
   * @param {number} timePeriodLength
   * @param {number} totalElapsedTime
   */
  _updateProgressMadeInTimePeriod(node, timePeriodLength, totalElapsedTime) {
    const auxData = this._nodeAuxiliaryData.get(node);
    const isFinished = auxData.estimatedTimeElapsed === timePeriodLength;

    if (node.type === Node.TYPES.CPU) {
      return isFinished
        ? this._markNodeAsComplete(node, totalElapsedTime)
        : (auxData.timeElapsed += timePeriodLength);
    }

    if (node.type !== Node.TYPES.NETWORK) throw new Error('Unsupported');

    const connection = this._connections.get(node.record.connectionId);
    const calculation = connection.calculateTimeToDownload(
      node.record.transferSize - auxData.bytesDownloaded,
      auxData.timeElapsed,
      timePeriodLength - auxData.timeElapsedOvershoot
    );

    connection.setCongestionWindow(calculation.congestionWindow);

    if (isFinished) {
      connection.setWarmed(true);
      this._connectionsInUse.delete(connection);
      this._markNodeAsComplete(node, totalElapsedTime);
    } else {
      auxData.timeElapsed += calculation.timeElapsed;
      auxData.timeElapsedOvershoot += calculation.timeElapsed - timePeriodLength;
      auxData.bytesDownloaded += calculation.bytesDownloaded;
    }
  }

  /**
   * @return {{timeInMs: number, nodeAuxiliaryData: !Map<!Node, Object>}}
   */
  estimateWithDetails() {
    // initialize all the necessary data containers
    this._initializeNetworkRecords();
    this._initializeNetworkConnections();
    this._initializeAuxiliaryData();

    const nodesUnprocessed = this._nodesUnprocessed;
    const nodesInQueue = this._nodesInQueue;
    const nodesInProcess = this._nodesInProcess;

    const rootNode = this._graph.getRootNode();
    rootNode.traverse(node => nodesUnprocessed.add(node));

    let depth = 0;
    let totalElapsedTime = 0;

    // add root node to queue
    this._markNodeAsInQueue(rootNode, totalElapsedTime);

    // loop as long as we have nodes in the queue or currently in process
    while (nodesInQueue.size || nodesInProcess.size) {
      depth++;

      // move all possible queued nodes to in process
      for (const node of nodesInQueue) {
        this._startNodeIfPossible(node, totalElapsedTime);
      }

      // set the available throughput for all connections based on # inflight
      this._updateNetworkCapacity();

      // find the time that the next node will finish
      const minimumTime = this._findNextNodeCompletionTime();
      totalElapsedTime += minimumTime;

      // update how far each node will progress until that point
      for (const node of nodesInProcess) {
        this._updateProgressMadeInTimePeriod(node, minimumTime, totalElapsedTime);
      }

      if (depth > 10000) {
        throw new Error('Maximum depth exceeded: estimate');
      }
    }

    if (nodesUnprocessed.size !== 0) {
      throw new Error(`Cycle detected: ${nodesUnprocessed.size} unused nodes`);
    }

    return {
      timeInMs: totalElapsedTime,
      nodeAuxiliaryData: this._nodeAuxiliaryData,
    };
  }

  /**
   * @return {number}
   */
  estimate() {
    return this.estimateWithDetails().timeInMs;
  }
}

module.exports = Estimator;
