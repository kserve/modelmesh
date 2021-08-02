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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;
import com.ibm.watson.prometheus.Gauge.TimeProvider;
import io.prometheus.client.Collector;
import io.prometheus.client.SimpleTimer;

/**
 * Histogram metric, to track distributions of events.
 * <p>
 * Example of uses for Histograms include:
 * <ul>
 *  <li>Response latency</li>
 *  <li>Request size</li>
 * </ul>
 * <p>
 * <em>Note:</em> Each bucket is one timeseries. Many buckets and/or many dimensions with labels
 * can produce large amount of time series, that may cause performance problems.
 *
 * <p>
 * The default buckets are intended to cover a typical web/rpc request from milliseconds to seconds.
 * <p>
 * Example Histograms:
 * <pre>
 * {@code
 *   class YourClass {
 *     static final Histogram requestLatency = Histogram.build()
 *         .name("requests_latency_seconds").help("Request latency in seconds.").register();
 *
 *     void processRequest(Request req) {
 *        Histogram.Timer requestTimer = requestLatency.startTimer();
 *        try {
 *          // Your code here.
 *        } finally {
 *          requestTimer.observeDuration();
 *        }
 *     }
 *
 *     // Or if using Java 8 lambdas.
 *     void processRequestLambda(Request req) {
 *        requestLatency.time(() -> {
 *          // Your code here.
 *        });
 *     }
 *   }
 * }
 * </pre>
 * <p>
 * You can choose your own buckets:
 * <pre>
 * {@code
 *     static final Histogram requestLatency = Histogram.build()
 *         .buckets(.01, .02, .03, .04)
 *         .name("requests_latency_seconds").help("Request latency in seconds.").register();
 * }
 * </pre>
 * {@link Histogram.Builder#linearBuckets(double, double, int) linearBuckets} and
 * {@link Histogram.Builder#exponentialBuckets(double, double, int) exponentialBuckets}
 * offer easy ways to set common bucket patterns.
 */
public class Histogram extends SimpleCollector<Histogram.Child> implements Collector.Describable {
  private final static TimeProvider defaultTimeProvider = new TimeProvider();

  private final double[] buckets;

  Histogram(Builder b) {
    super(b);
    buckets = b.buckets;
    initializeNoLabelsChild();

    bucketName = fullname + "_bucket";
    countName = fullname + "_count";
    sumName = fullname + "_sum";
    labelNamesWithLe = new ArrayList<String>(labelNames);
    labelNamesWithLe.add("le");
    ((ArrayList<String>) labelNamesWithLe).trimToSize();
  }

  public static class Builder extends SimpleCollector.Builder<Builder, Histogram> {
    private double[] buckets = new double[]{.005, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10};

    @Override
    public Histogram create() {
      for (int i = 0; i < buckets.length - 1; i++) {
        if (buckets[i] >= buckets[i + 1]) {
          throw new IllegalStateException("Histogram buckets must be in increasing order: "
              + buckets[i] + " >= " + buckets[i + 1]);
        }
      }
      if (buckets.length == 0) {
          throw new IllegalStateException("Histogram must have at least one bucket.");
      }
      for (String label: labelNames) {
        if (label.equals("le")) {
            throw new IllegalStateException("Histogram cannot have a label named 'le'.");
        }
      }

      // Append infinity bucket if it's not already there.
      if (buckets[buckets.length - 1] != Double.POSITIVE_INFINITY) {
        double[] tmp = new double[buckets.length + 1];
        System.arraycopy(buckets, 0, tmp, 0, buckets.length);
        tmp[buckets.length] = Double.POSITIVE_INFINITY;
        buckets = tmp;
      }
      dontInitializeNoLabelsChild = true;
      return new Histogram(this);
    }

    /**
      * Set the upper bounds of buckets for the histogram.
      */
    public Builder buckets(double... buckets) {
      this.buckets = buckets;
      return this;
    }

    /**
      * Set the upper bounds of buckets for the histogram with a linear sequence.
      */
    public Builder linearBuckets(double start, double width, int count) {
      buckets = new double[count];
      for (int i = 0; i < count; i++){
        buckets[i] = start + i*width;
      }
      return this;
    }
    /**
      * Set the upper bounds of buckets for the histogram with an exponential sequence.
      */
    public Builder exponentialBuckets(double start, double factor, int count) {
      buckets = new double[count];
      for (int i = 0; i < count; i++) {
        buckets[i] = start * Math.pow(factor, i);
      }
      return this;
    }

  }

  /**
   *  Return a Builder to allow configuration of a new Histogram. Ensures required fields are provided.
   *
   *  @param name The name of the metric
   *  @param help The help string of the metric
   */
  public static Builder build(String name, String help) {
    return new Builder().name(name).help(help);
  }

  /**
   *  Return a Builder to allow configuration of a new Histogram.
   */
  public static Builder build() {
    return new Builder();
  }

  @Override
  protected Child newChild() {
    throw new IllegalStateException();
  }

  @Override
  protected Child newChild(String[] labelValues) {
    return new Child(buckets, labelValues);
  }

  /**
   * Represents an event being timed.
   */
  public static class Timer implements Closeable {
    private final Child child;
    private final long start;
    private Timer(Child child, long start) {
      this.child = child;
      this.start = start;
    }
    /**
     * Observe the amount of time in seconds since {@link Child#startTimer} was called.
     * @return Measured duration in seconds since {@link Child#startTimer} was called.
     */
    public double observeDuration() {
        double elapsed = SimpleTimer.elapsedSecondsFromNanos(start, defaultTimeProvider.nanoTime());
        child.observe(elapsed);
        return elapsed;
    }

    /**
     * Equivalent to calling {@link #observeDuration()}.
     */
    @Override
    public void close() {
      observeDuration();
    }
  }

  /**
   * The value of a single Histogram.
   * <p>
   * <em>Warning:</em> References to a Child become invalid after using
   * {@link SimpleCollector#remove} or {@link SimpleCollector#clear}.
   */
  public static class Child {

    /**
     * Executes runnable code (e.g. a Java 8 Lambda) and observes a duration of how long it took to run.
     *
     * @param timeable Code that is being timed
     * @return Measured duration in seconds for timeable to complete.
     */
    public double time(Runnable timeable) {
      Timer timer = startTimer();

      double elapsed;
      try {
        timeable.run();
      } finally {
        elapsed = timer.observeDuration();
      }
      return elapsed;
    }

    /**
     * Executes callable code (e.g. a Java 8 Lambda) and observes a duration of how long it took to run.
     *
     * @param timeable Code that is being timed
     * @return Result returned by callable.
     */
    public <E> E time(Callable<E> timeable) {
      Timer timer = startTimer();

      try {
        return timeable.call();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        timer.observeDuration();
      }
    }

    public static class Value {
      public double sum;
      public final double[] buckets;

      public Value(double sum, double[] buckets) {
        this.sum = sum;
        this.buckets = buckets;
      }
    }

    @SuppressWarnings("unchecked")
    private Child(double[] buckets, String[] labelValues) {
      upperBounds = buckets;
      cumulativeCounts = new LongAdder[buckets.length];
      for (int i = 0; i < buckets.length; ++i) {
        cumulativeCounts[i] = new LongAdder();
      }
      labelValuesWithLe = new List[buckets.length];
      for (int i = 0; i < buckets.length; i++) {
        String[] array = Arrays.copyOf(labelValues, labelValues.length + 1);
        array[labelValues.length] = doubleToGoString(buckets[i]);
        labelValuesWithLe[i] = Arrays.asList(array);
      }
    }
    private final double[] upperBounds;
    private final LongAdder[] cumulativeCounts;
    private final DoubleAdder sum = new DoubleAdder();

    final List<String>[] labelValuesWithLe;

    /**
     * Observe the given amount.
     */
    public void observe(double amt) {
      for (int i = 0; i < upperBounds.length; ++i) {
        // The last bucket is +Inf, so we always increment.
        if (amt <= upperBounds[i]) {
          cumulativeCounts[i].add(1L);
          break;
        }
      }
      sum.add(amt);
    }
    /**
     * Observe the given amount.
     */
    public void observeBS(double amt) {
      //TODO bench binary versus linear search for different sizes
      int i = Arrays.binarySearch(upperBounds, amt);
      cumulativeCounts[i >= 0 ? i : (-i -1)].add(1L);
      sum.add(amt);
    }
    /**
     * Start a timer to track a duration.
     * <p>
     * Call {@link Timer#observeDuration} at the end of what you want to measure the duration of.
     */
    public Timer startTimer() {
      return new Timer(this, defaultTimeProvider.nanoTime());
    }
    /**
     * Get the value of the Histogram.
     * <p>
     * <em>Warning:</em> The definition of {@link Value} is subject to change.
     */
    public Value get() {
      return get(null);
    }
    /**
     * Get the value of the Histogram.
     * <p>
     * <em>Warning:</em> The definition of {@link Value} is subject to change.
     *
     * @param value a {@link Value} object to fill with data
     */
    public Value get(Value value) {
      if (value == null || value.buckets.length != cumulativeCounts.length) {
        value = new Value(0.0, new double[cumulativeCounts.length]);
      }
      long acc = 0L;
      for (int i = 0; i < cumulativeCounts.length; ++i) {
        acc += cumulativeCounts[i].sum();
        value.buckets[i] = acc;
      }
      value.sum = sum.sum();
      return value;
    }
  }

  // Convenience methods.
  /**
   * Observe the given amount on the histogram with no labels.
   */
  public void observe(double amt) {
    noLabelsChild.observe(amt);
  }
  /**
   * Start a timer to track a duration on the histogram with no labels.
   * <p>
   * Call {@link Timer#observeDuration} at the end of what you want to measure the duration of.
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
  public double time(Runnable timeable){
    return noLabelsChild.time(timeable);
  }

  /**
   * Executes callable code (e.g. a Java 8 Lambda) and observes a duration of how long it took to run.
   *
   * @param timeable Code that is being timed
   * @return Result returned by callable.
   */
  public <E> E time(Callable<E> timeable){
    return noLabelsChild.time(timeable);
  }

  private final String bucketName, countName, sumName;

  private final List<String> labelNamesWithLe;

  @Override
  public List<MetricFamilySamples> collect() {
    final Map.Entry<List<String>, Child>[] children = children();
    int count = children.length * (buckets.length + 2);
    List<MetricFamilySamples.Sample> samples = new ArrayList<MetricFamilySamples.Sample>(count);
    Child.Value v = null; //TODO maybe threadlocal
    for(Map.Entry<List<String>, Child> c: children) {
      if (c == null) {
        continue;
      }
      v = c.getValue().get(v);
      List<String>[] labelValuesWithLe = c.getValue().labelValuesWithLe;
      for (int i = 0; i < v.buckets.length; ++i) {
        samples.add(new MetricFamilySamples.Sample(bucketName, labelNamesWithLe, labelValuesWithLe[i], v.buckets[i]));
      }
      samples.add(new MetricFamilySamples.Sample(countName, labelNames, c.getKey(), v.buckets[buckets.length-1]));
      samples.add(new MetricFamilySamples.Sample(sumName, labelNames, c.getKey(), v.sum));
    }

    return familySamplesList(Type.HISTOGRAM, samples);
  }

  @Override
  public List<MetricFamilySamples> describe() {
    return Collections.singletonList(
            new MetricFamilySamples(fullname, Type.HISTOGRAM, help, Collections.<MetricFamilySamples.Sample>emptyList()));
  }

  double[] getBuckets() {
    return buckets;
  }


}
