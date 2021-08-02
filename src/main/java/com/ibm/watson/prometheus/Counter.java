/*
 * Copyright 2021 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

/*
 * This is a modified (optimized) version of the equivalent class from the official
 * Apache 2.0 licensed prometheus java client https://github.com/prometheus/client_java,
 * which is also a dependency.
 *
 * The current mods are based on master-branch commit b61dd232a504e20dad404a2bf3e2c0b8661c212a
 */
package com.ibm.watson.prometheus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.DoubleAdder;
import io.prometheus.client.Collector;
import io.prometheus.client.CounterMetricFamily;

/**
 * Counter metric, to track counts of events or running totals.
 * <p>
 * Example of Counters include:
 * <ul>
 *  <li>Number of requests processed</li>
 *  <li>Number of items that were inserted into a queue</li>
 *  <li>Total amount of data a system has processed</li>
 * </ul>
 *
 * Counters can only go up (and be reset), if your use case can go down you should use a {@link Gauge} instead.
 * Use the <code>rate()</code> function in Prometheus to calculate the rate of increase of a Counter.
 * By convention, the names of Counters are suffixed by <code>_total</code>.
 *
 * <p>
 * An example Counter:
 * <pre>
 * {@code
 *   class YourClass {
 *     static final Counter requests = Counter.build()
 *         .name("requests_total").help("Total requests.").register();
 *     static final Counter failedRequests = Counter.build()
 *         .name("requests_failed_total").help("Total failed requests.").register();
 *
 *     void processRequest() {
 *        requests.inc();
 *        try {
 *          // Your code here.
 *        } catch (Exception e) {
 *          failedRequests.inc();
 *          throw e;
 *        }
 *     }
 *   }
 * }
 * </pre>
 *
 * <p>
 * You can also use labels to track different types of metric:
 * <pre>
 * {@code
 *   class YourClass {
 *     static final Counter requests = Counter.build()
 *         .name("requests_total").help("Total requests.")
 *         .labelNames("method").register();
 *
 *     void processGetRequest() {
 *        requests.labels("get").inc();
 *        // Your code here.
 *     }
 *     void processPostRequest() {
 *        requests.labels("post").inc();
 *        // Your code here.
 *     }
 *   }
 * }
 * </pre>
 * These can be aggregated and processed together much more easily in the Prometheus
 * server than individual metrics for each labelset.
 */
public class Counter extends SimpleCollector<Counter.Child> implements Collector.Describable {

  Counter(Builder b) {
    super(b);
  }

  public static class Builder extends SimpleCollector.Builder<Builder, Counter> {
    @Override
    public Counter create() {
      return new Counter(this);
    }
  }

  /**
   *  Return a Builder to allow configuration of a new Counter. Ensures required fields are provided.
   *
   *  @param name The name of the metric
   *  @param help The help string of the metric
   */
  public static Builder build(String name, String help) {
    return new Builder().name(name).help(help);
  }

  /**
   *  Return a Builder to allow configuration of a new Counter.
   */
  public static Builder build() {
    return new Builder();
  }

  @Override
  protected Child newChild() {
    return new Child();
  }

  /**
   * The value of a single Counter.
   * <p>
   * <em>Warning:</em> References to a Child become invalid after using
   * {@link SimpleCollector#remove} or {@link SimpleCollector#clear},
   */
  public static class Child {
    private final DoubleAdder value = new DoubleAdder();
    /**
     * Increment the counter by 1.
     */
    public void inc() {
      inc(1.0);
    }
    /**
     * Increment the counter by the given amount.
     * @throws IllegalArgumentException If amt is negative.
     */
    public void inc(double amt) {
      if (amt < 0.0) {
        throw new IllegalArgumentException("Amount to increment must be non-negative.");
      }
      value.add(amt);
    }
    /**
     * Get the value of the counter.
     */
    public double get() {
      return value.sum();
    }
  }

  // Convenience methods.
  /**
   * Increment the counter with no labels by 1.
   */
  public void inc() {
    inc(1.0);
  }
  /**
   * Increment the counter with no labels by the given amount.
   * @throws IllegalArgumentException If amt is negative.
   */
  public void inc(double amt) {
    noLabelsChild.inc(amt);
  }

  /**
   * Get the value of the counter.
   */
  public double get() {
    return noLabelsChild.get();
  }

  @Override
  public List<MetricFamilySamples> collect() {
    final Map.Entry<List<String>, Child>[] children = children();
    List<MetricFamilySamples.Sample> samples = new ArrayList<MetricFamilySamples.Sample>(children.length);
    for(Map.Entry<List<String>, Child> c : children) {
      if (c != null) {
        samples.add(new MetricFamilySamples.Sample(fullname, labelNames, c.getKey(), c.getValue().get()));
      }
    }
    return familySamplesList(Type.COUNTER, samples);
  }

  @Override
  public List<MetricFamilySamples> describe() {
    return Collections.<MetricFamilySamples>singletonList(new CounterMetricFamily(fullname, help, labelNames));
  }
}
