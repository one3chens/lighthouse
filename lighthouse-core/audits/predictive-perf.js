/**
 * @license Copyright 2017 Google Inc. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
'use strict';

const Audit = require('./audit');
const Util = require('../report/v2/renderer/util.js');
const PageDependencyGraph = require('../gather/computed/page-dependency-graph.js');
const Node = require('../gather/computed/dependency-graph/node.js');

// Parameters (in ms) for log-normal CDF scoring. To see the curve:
//   https://www.desmos.com/calculator/rjp0lbit8y
const SCORING_POINT_OF_DIMINISHING_RETURNS = 1700;
const SCORING_MEDIAN = 10000;

// Any CPU task of 20 ms or more will end up being a critical long task on mobile
const CRITICAL_LONG_TASK_THRESHOLD = 20000;

class PredictivePerf extends Audit {
  /**
   * @return {!AuditMeta}
   */
  static get meta() {
    return {
      category: 'Performance',
      name: 'predictive-perf',
      description: 'Predicted Performance (beta)',
      helpText: '',
      requiredArtifacts: ['traces', 'devtoolsLogs']
    };
  }

  /**
   * @param {!Node} graph
   * @param {!TraceOfTabArtifact} traceOfTab
   * @return {!Node}
   */
  static getOptimisticFMPGraph(graph, traceOfTab) {
    const fmp = traceOfTab.timestamps.firstMeaningfulPaint;
    return graph.cloneWithRelationships(node => {
      if (node.endTime > fmp || node.type === Node.TYPES.CPU) return false;
      // Include non-script-initiated network requests with a render-blocking priority
      return node.hasRenderBlockingPriority() && node.initiatorType !== 'script';
    });
  }

  /**
   * @param {!Node} graph
   * @param {!TraceOfTabArtifact} traceOfTab
   * @return {!Node}
   */
  static getPessimisticFMPGraph(graph, traceOfTab) {
    const fmp = traceOfTab.timestamps.firstMeaningfulPaint;
    return graph.cloneWithRelationships(node => {
      if (node.endTime > fmp) return false;
      // Include CPU tasks that performed a layout
      if (node.type === Node.TYPES.CPU) return node.didPerformLayout();
      // Include all network requests that had render-blocking priority (even script-initiated)
      return node.hasRenderBlockingPriority();
    });
  }

  /**
   * @param {!Node} graph
   * @return {!Node}
   */
  static getOptimisticTTCIGraph(graph) {
    return graph.cloneWithRelationships(node => {
      // Include everything that might be a long task
      if (node.type === Node.TYPES.CPU) return node.event.dur > CRITICAL_LONG_TASK_THRESHOLD;
      // Include all scripts and high priority requests
      return node.resourceType !== 'image' && (node.resourceType === 'script' ||
          node.record.priority() === 'High' ||
          node.record.priority() === 'VeryHigh');
    });
  }

  /**
   * @param {!Node} graph
   * @return {!Node}
   */
  static getPessimisticTTCIGraph(graph) {
    return graph;
  }

  /**
   * @param {!Map<!Node, {startTime, endTime}>} nodeAuxData
   * @return {number}
   */
  static getLastLongTaskEndTime(nodeAuxData) {
    return Array.from(nodeAuxData.entries())
        .filter(([node, auxData]) => node.type === Node.TYPES.CPU &&
            auxData.endTime - auxData.startTime > 50)
        .map(([_, auxData]) => auxData.endTime)
        .reduce((max, x) => Math.max(max, x), 0);
  }

  /**
   * @param {!Artifacts} artifacts
   * @return {!AuditResult}
   */
  static audit(artifacts) {
    const trace = artifacts.traces[Audit.DEFAULT_PASS];
    const devtoolsLogs = artifacts.devtoolsLogs[Audit.DEFAULT_PASS];
    return Promise.all([
      artifacts.requestPageDependencyGraph(trace, devtoolsLogs),
      artifacts.requestTraceOfTab(trace),
    ]).then(([graph, traceOfTab]) => {
      const graphs = {
        optimisticFMP: PredictivePerf.getOptimisticFMPGraph(graph, traceOfTab),
        pessimisticFMP: PredictivePerf.getPessimisticFMPGraph(graph, traceOfTab),
        optimisticTTCI: PredictivePerf.getOptimisticTTCIGraph(graph, traceOfTab),
        pessimisticTTCI: PredictivePerf.getPessimisticTTCIGraph(graph, traceOfTab),
      };

      let sum = 0;
      const values = {};
      Object.keys(graphs).forEach(key => {
        const estimate = PageDependencyGraph.estimateGraph(graphs[key]);
        const lastLongTaskEnd = PredictivePerf.getLastLongTaskEndTime(estimate.nodeAuxiliaryData);

        switch (key) {
          case 'optimisticFMP':
          case 'pessimisticFMP':
            values[key] = estimate.timeInMs;
            break;
          case 'optimisticTTCI':
            values[key] = Math.max(values.optimisticFMP, lastLongTaskEnd);
            break;
          case 'pessimisticTTCI':
            values[key] = Math.max(values.pessimisticFMP, lastLongTaskEnd);
            break;
        }

        sum += values[key];
      });

      const rawValue = sum / 4;
      const score = Audit.computeLogNormalScore(
        rawValue,
        SCORING_POINT_OF_DIMINISHING_RETURNS,
        SCORING_MEDIAN
      );

      return {
        score,
        rawValue,
        displayValue: Util.formatMilliseconds(rawValue),
        extendedInfo: {value: values},
      };
    });
  }
}

module.exports = PredictivePerf;
