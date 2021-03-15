package com.danielor.beamstats;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn.Accumulator;

/**
 * {@code PTransform}s for computing variance of elements in a {@code PCollection} or
 * the variance of a value by key in a {@code
 */
public class Variance {

  private static final double EPSILON = 1e-7; // For floating point comparisons;

  private Variance() {}

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<N>} and returns a
   * {@code PCollection<Double>} whose content is the variance of the input.
   *
   * @param <N> the type of {@code Number}
   * @return
   */
  public static <N extends Number> Combine.Globally<N, Double> globally() {
    return Combine.globally(Variance.of());
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K,N>>} and returns
   * a {@code PCollection<KV<K, Double>>} that computes the variance for each key K.
   *
   * @param <K> The type of keys
   * @param <N> The type of {@code Number}s
   * @return
   */
  public static <K, N extends Number> Combine.PerKey<K, N, Double> perKey() {
    return Combine.perKey(Variance.of());
  }

  /**
   * Creates a {@code Combine.CombineFn} that computes the variance of a {@code PCollection}
   *
   * @param <N> the type of {@code Number}
   * @return
   */
  public static <N extends Number> Combine.AccumulatingCombineFn<N, VarianceAccumulator<N>, Double> of() {
    return new VarianceFunction<>();
  }

  /**
   * The variance function creates the accumulator and coder needed by the Apache Beam combine operator
   * @param <N>
   */
  private static class VarianceFunction<N extends Number>
    extends Combine.AccumulatingCombineFn<N, VarianceAccumulator<N>, Double> {

    @Override
    public VarianceAccumulator<N> createAccumulator() {
      return new VarianceAccumulator<>();
    }

    @Override
    public Coder<VarianceAccumulator<N>> getAccumulatorCoder(
      CoderRegistry registry,
      Coder<N> inputCoder
    ) {
      return new VarianceCoder<>();
    }
  }

  /**
   * An accumulator class that calculates the variance E[X^2] - (E[X])^2
   * @param <N> Number
   */
  static class VarianceAccumulator<N extends Number>
    implements Accumulator<N, VarianceAccumulator<N>, Double> {

    long count = 0;
    double sum = 0.0;
    double sumsq = 0.0;

    public VarianceAccumulator() {
      this(0, 0.0, 0.0);
    }

    public VarianceAccumulator(long count, double sum, double sumsq) {
      this.count = count;
      this.sum = sum;
      this.sumsq = sumsq;
    }

    private boolean doubleEquals(double f1, double f2, double epsilon) {
      return Math.abs(f1 - f2) < epsilon;
    }

    @Override
    public void addInput(N input) {
      count++;
      double value = input.doubleValue();
      sum += value;
      sumsq += value * value;
    }

    @Override
    public void mergeAccumulator(VarianceAccumulator<N> accumulator) {
      count += accumulator.count;
      sum += accumulator.sum;
      sumsq += accumulator.sumsq;
    }

    @Override
    public Double extractOutput() {
      if (count == 0) {
        return Double.NaN;
      }
      double mean = sum / count;
      return (sumsq / count) - mean * mean;
    }

    @Override
    public int hashCode() {
      return Objects.hash(count, sum, sumsq);
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof VarianceAccumulator)) {
        return false;
      }
      VarianceAccumulator<?> otherAccum = (VarianceAccumulator<?>) other;
      return (
        (count == otherAccum.count) &&
        this.doubleEquals(sum, otherAccum.sum, EPSILON) &&
        this.doubleEquals(sumsq, otherAccum.sumsq, EPSILON)
      );
    }

    @Override
    public String toString() {
      return (
        String.valueOf(count) +
        "," +
        String.valueOf(sum) +
        "," +
        String.valueOf(sumsq)
      );
    }
  }

  /**
   * A coder used to transform the accumulator into a format that Apache beam
   * can understand.
   * @param <N> Number
   */
  static class VarianceCoder<N extends Number>
    extends AtomicCoder<VarianceAccumulator<N>> {

    private static final long serialVersionUID = -3413678295720268029L;
    private static final Coder<Long> LONG_CODER = BigEndianLongCoder.of();
    private static final Coder<Double> DOUBLE_CODER = DoubleCoder.of();

    @Override
    public void encode(VarianceAccumulator<N> value, OutputStream outStream)
      throws CoderException, IOException {
      LONG_CODER.encode(value.count, outStream);
      DOUBLE_CODER.encode(value.sum, outStream);
      DOUBLE_CODER.encode(value.sumsq, outStream);
    }

    @Override
    public VarianceAccumulator<N> decode(InputStream inStream)
      throws CoderException, IOException {
      return new VarianceAccumulator<>(
        LONG_CODER.decode(inStream),
        DOUBLE_CODER.decode(inStream),
        DOUBLE_CODER.decode(inStream)
      );
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      LONG_CODER.verifyDeterministic();
      DOUBLE_CODER.verifyDeterministic();
    }
  }
}
