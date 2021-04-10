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
 * {@code PTransform}s for computing moments of elements in a {@code PCollection} or
 * the central moment of a value by key in a {@code KV}
 */
public class Moment {

  private static final double EPSILON = 1e-7; // For floating point comparisons;

  private Moment() {}
  
  /**
   * An enum that encapsulates the different moments
   */
  public enum MomentType {
    MEAN,
    VARIANCE
  };

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<N>} and returns a
   * {@code PCollection<Double>} whose content is the variance of the input.
   *
   * @param type The moment type (mean, variance, skew, etc.)
   * @param <N> the type of {@code Number}
   * @return
   */
  public static <N extends Number> Combine.Globally<N, Double> globally(MomentType type) {
    return Combine.globally(Moment.of(type));
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K,N>>} and returns
   * a {@code PCollection<KV<K, Double>>} that computes the variance for each key K.
   *
   * @param type The moment type (mean, variance, skew, etc.)
   * @param <K> The type of keys
   * @param <N> The type of {@code Number}s
   * @return
   */
  public static <K, N extends Number> Combine.PerKey<K, N, Double> perKey(MomentType type) {
    return Combine.perKey(Moment.of(type));
  }

  /**
   * Creates a {@code Combine.CombineFn} that computes the variance of a {@code PCollection}
   *
   * @param type The moment type (mean, variance, skew, etc.)
   * @param <N> the type of {@code Number}
   * @return
   */
  public static <N extends Number> Combine.AccumulatingCombineFn<N, MomentAccumulator<N>, Double> of(MomentType type) {
    return new VarianceFunction<>(type);
  }

  /**
   * The variance function creates the accumulator and coder needed by the Apache Beam combine operator
   * @param <N>
   */
  private static class VarianceFunction<N extends Number>
    extends Combine.AccumulatingCombineFn<N, MomentAccumulator<N>, Double> {

    MomentType type;
    
    public VarianceFunction(MomentType t) {
      type = t;
    }
    
    @Override
    public MomentAccumulator<N> createAccumulator() {
      return new MomentAccumulator<>(type);
    }

    @Override
    public Coder<MomentAccumulator<N>> getAccumulatorCoder(
      CoderRegistry registry,
      Coder<N> inputCoder
    ) {
      return new MomentCoder<>();
    }
  }

  /**
   * An accumulator class that calculates the variance E[X^2] - (E[X])^2
   * @param <N> Number
   */
  static class MomentAccumulator<N extends Number>
    implements Accumulator<N, MomentAccumulator<N>, Double> {

    long count = 0;
    double sum = 0.0;
    double sumsq = 0.0;
    MomentType type = MomentType.MEAN;

    public MomentAccumulator() {
      this(0, 0.0, 0.0);
    }
    
    public MomentAccumulator(MomentType t) {
      this.type = t;
    }

    public MomentAccumulator(long count, double sum, double sumsq) {
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
    public void mergeAccumulator(MomentAccumulator<N> accumulator) {
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
      if (type == MomentType.MEAN) {
        return mean;
      } else {
        return (sumsq / count) - mean * mean;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(count, sum, sumsq);
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof MomentAccumulator)) {
        return false;
      }
      MomentAccumulator<?> otherAccum = (MomentAccumulator<?>) other;
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
  static class MomentCoder<N extends Number>
    extends AtomicCoder<MomentAccumulator<N>> {

    private static final long serialVersionUID = -3413678295720268029L;
    private static final Coder<Long> LONG_CODER = BigEndianLongCoder.of();
    private static final Coder<Double> DOUBLE_CODER = DoubleCoder.of();

    @Override
    public void encode(MomentAccumulator<N> value, OutputStream outStream)
      throws CoderException, IOException {
      LONG_CODER.encode(value.count, outStream);
      DOUBLE_CODER.encode(value.sum, outStream);
      DOUBLE_CODER.encode(value.sumsq, outStream);
    }

    @Override
    public MomentAccumulator<N> decode(InputStream inStream)
      throws CoderException, IOException {
      return new MomentAccumulator<>(
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
