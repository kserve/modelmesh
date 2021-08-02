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
import java.util.concurrent.Callable;
import io.prometheus.client.Collector;
import io.prometheus.client.Gauge.Child;
import io.prometheus.client.Gauge.Timer;
import io.prometheus.client.GaugeMetricFamily;

/**
 * Gauge metric, to report instantaneous values.
 * <p>
 * Examples of Gauges include:
 * <ul>
 *  <li>Inprogress requests</li>
 *  <li>Number of items in a queue</li>
 *  <li>Free memory</li>
 *  <li>Total memory</li>
 *  <li>Temperature</li>
 * </ul>
 *
 * Gauges can go both up and down.
 * <p>
 * An example Gauge:
 * <pre>
 * {@code
 *   class YourClass {
 *     static final Gauge inprogressRequests = Gauge.build()
 *         .name("inprogress_requests").help("Inprogress requests.").register();
 *
 *     void processRequest() {
 *        inprogressRequests.inc();
 *        // Your code here.
 *        inprogressRequests.dec();
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
 *     static final Gauge inprogressRequests = Gauge.build()
 *         .name("inprogress_requests").help("Inprogress requests.")
 *         .labelNames("method").register();
 *
 *     void processGetRequest() {
 *        inprogressRequests.labels("get").inc();
 *        // Your code here.
 *        inprogressRequests.labels("get").dec();
 *     }
 *     void processPostRequest() {
 *        inprogressRequests.labels("post").inc();
 *        // Your code here.
 *        inprogressRequests.labels("post").dec();
 *     }
 *   }
 * }
 * </pre>
 * <p>
 * These can be aggregated and processed together much more easily in the Prometheus
 * server than individual metrics for each labelset.
 */
public class Gauge extends SimpleCollector<io.prometheus.client.Gauge.Child> implements Collector.Describable {

  Gauge(Builder b) {
    super(b);
  }

  public static class Builder extends SimpleCollector.Builder<Builder, Gauge> {
    @Override
    public Gauge create() {
      return new Gauge(this);
    }
  }

  /**
   *  Return a Builder to allow configuration of a new Gauge. Ensures required fields are provided.
   *
   *  @param name The name of the metric
   *  @param help The help string of the metric
   */
  public static Builder build(String name, String help) {
    return new Builder().name(name).help(help);
  }

  /**
   *  Return a Builder to allow configuration of a new Gauge.
   */
  public static Builder build() {
    return new Builder();
  }

  @Override
  protected Child newChild() {
    return new Child();
  }

  // Convenience methods.
  /**
   * Increment the gauge with no labels by 1.
   */
  public void inc() {
    inc(1.0);
  }
  /**
   * Increment the gauge with no labels by the given amount.
   */
  public void inc(double amt) {
    noLabelsChild.inc(amt);
  }
  /**
   * Decrement the gauge with no labels by 1.
   */
  public void dec() {
    dec(1.0);
  }
  /**
   * Decrement the gauge with no labels by the given amount.
   */
  public void dec(double amt) {
    noLabelsChild.dec(amt);
  }
  /**
   * Set the gauge with no labels to the given value.
   */
  public void set(double val) {
    noLabelsChild.set(val);
  }
  /**
   * Set the gauge with no labels to the current unixtime.
   */
  public void setToCurrentTime() {
    noLabelsChild.setToCurrentTime();
  }
  /**
   * Start a timer to track a duration, for the gauge with no labels.
   * <p>
   * This is primarily useful for tracking the durations of major steps of batch jobs,
   * which are then pushed to a PushGateway.
   * For tracking other durations/latencies you should usually use a {@link Summary}.
   * <p>
   * Call {@link Timer#setDuration} at the end of what you want to measure the duration of.
   */
  public Timer startTimer() {
    return noLabelsChild.startTimer();
  }

  /**
   * Executes runnable code (e.g. a Java 8 Lambda) and observes a duration of how long it took to run.
   *
   * @param timeable Code that is being timed
   * @return Measured duration in seconds for timeable to complete.
   */
  public double setToTime(Runnable timeable){
    return noLabelsChild.setToTime(timeable);
  }

  /**
   * Executes callable code (e.g. a Java 8 Lambda) and observes a duration of how long it took to run.
   *
   * @param timeable Code that is being timed
   * @return Result returned by callable.
   */
  public <E> E setToTime(Callable<E> timeable){
    return noLabelsChild.setToTime(timeable);
  }

  /**
   * Get the value of the gauge.
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
    return familySamplesList(Type.GAUGE, samples);
  }

  @Override
  public List<MetricFamilySamples> describe() {
    return Collections.<MetricFamilySamples>singletonList(new GaugeMetricFamily(fullname, help, labelNames));
  }

  static class TimeProvider {
    long currentTimeMillis() {
      return System.currentTimeMillis();
    }
    long nanoTime() {
      return System.nanoTime();
    }
  }
}
