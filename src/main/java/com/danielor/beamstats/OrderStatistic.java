package com.danielor.beamstats;

import java.util.Collections;
import java.util.PriorityQueue;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn.Accumulator;

/**
 * {@code PTransform} for computing order statistics (kth largest, kth smallest) in a
 * {@code PCollection}. It should be noted that the implemented approach will only work
 * for small k. It is inadvisable to use this transform for large k.
 */
public class OrderStatistic {

  private OrderStatistic() {}

  /**
   * An enum that encapsulates the different categories of order statistics.
   */
  public enum OrderType {
    SMALLEST,
    LARGEST,
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<N>} and returns a
   * {code PCollection<Double>} whose content is the kth largest element. If k is too large
   * to fit in memory, the transform will return NaN.
   *
   * @param <N> A number object
   * @param k A number
   * @return
   */
  public static <N extends Number> Combine.Globally<N, Number> kthLargest(
    int k
  ) {
    return Combine.globally(OrderStatistic.of(OrderType.LARGEST, k));
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<N>} and returns a
   * {code PCollection<Double>} whose content is the kth smallest element. If k is too large
   * to fit in memory, the transform will return NaN.
   *
   * @param <N> A number object
   * @param k A number
   * @return
   */
  public static <N extends Number> Combine.Globally<N, Number> kthSmallest(
    int k
  ) {
    return Combine.globally(OrderStatistic.of(OrderType.SMALLEST, k));
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K,N>>} and returns
   * a {@code PCollection<KV<K, Double>>} that computes the kth largest for each key K.
   *
   * @param k The kth value
   * @param <K> The type of keys
   * @param <N> The number
   * @return
   */
  public static <K, N extends Number> Combine.PerKey<K, N, Number> kthLargestPerKey(
    int k
  ) {
    return Combine.perKey(OrderStatistic.of(OrderType.LARGEST, k));
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K,N>>} and returns
   * a {@code PCollection<KV<K, Double>>} that computes the kth smallest for each key K.
   *
   * @param k The kth value
   * @param <K> The type of keys
   * @param <N> The number
   * @return
   */
  public static <K, N extends Number> Combine.PerKey<K, N, Number> kthSmallestPerKey(
    int k
  ) {
    return Combine.perKey(OrderStatistic.of(OrderType.SMALLEST, k));
  }

  /**
   * Creates a {@code Combine.CombineFn} that computes the variance of a {@code PCollection}
   *
   * @param type The order type (min, max, etc.)
   * @param k k The kth value
   * @param <N> the type of {@code Number}
   * @return
   */
  public static <N extends Number> Combine.AccumulatingCombineFn<N, OrderAccumulator<N>, Number> of(
    OrderType type,
    int k
  ) {
    return new OrderFunction<>(type, k);
  }

  /**
   * The order function creates the accumulator and coder needed by the Apache Beam combine operator
   * @param <N>
   */
  private static class OrderFunction<N extends Number>
    extends Combine.AccumulatingCombineFn<N, OrderAccumulator<N>, Number> {

    private OrderType type;
    private int kth;

    public OrderFunction(OrderType t, int k) {
      type = t;
      kth = k;
    }

    @Override
    public OrderAccumulator<N> createAccumulator() {
      return new OrderAccumulator<>(type, kth);
    }
  }

  /**
   * An accumulator class that calculates the kth largest
   * @param <N> Number
   */
  private static class OrderAccumulator<N extends Number>
    implements Accumulator<N, OrderAccumulator<N>, Number> {

    private OrderType type;
    private int kth;
    private PriorityQueue<N> pq;

    public OrderAccumulator(OrderType t, int k) {
      type = t;
      kth = k;
      if (type == OrderType.SMALLEST) {
        pq = new PriorityQueue<>(Collections.reverseOrder());
      } else {
        pq = new PriorityQueue<>();
      }
    }

    @Override
    public void addInput(N input) {
      pq.add(input);
      if (pq.size() > kth) {
        pq.remove();
      }
    }

    @Override
    public void mergeAccumulator(OrderAccumulator<N> other) {
      PriorityQueue<N> opq = other.pq;
      while (opq.size() != 0) {
        pq.add(opq.poll());
        if (pq.size() > kth) {
          pq.remove();
        }
      }
    }

    @Override
    public N extractOutput() {
      if (pq == null) {
        return null;
      }
      return pq.peek();
    }
  }
}
