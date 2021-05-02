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
import org.apache.beam.sdk.coders.StringUtf8Coder;
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
    VARIANCE,
    SKEW,
    KURTOSIS
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
    double M1 = 0.0;
    double M2 = 0.0;
    double M3 = 0.0;
    double M4 = 0.0;
    MomentType type = MomentType.MEAN;

    public MomentAccumulator() {
      this(0, MomentType.MEAN.toString(), 0.0, 0.0, 0.0, 0.0);
    }
    
    public MomentAccumulator(MomentType t) {
      this.type = t;
    }

    public MomentAccumulator(long count, String type, double M1, double M2, double M3, double M4) {
      this.count = count;
      this.type = MomentType.valueOf(type);
      this.M1 = M1;
      this.M2 = M2;
      this.M3 = M3;
      this.M4 = M4;
    }

    private boolean doubleEquals(double f1, double f2, double epsilon) {
      return Math.abs(f1 - f2) < epsilon;
    }

    @Override
    public void addInput(N input) {
      long n = count;
      count++;
      
      double value = input.doubleValue();
      double delta = value - M1;
      double deltaN = delta / count;
      double deltaN2 = deltaN * deltaN;
      double deltaM2 = delta * deltaN * n;
      M4 += deltaM2 * deltaN2 * (count * count - 3 * count + 3) + 6 * deltaN2 * M2 - 4 * deltaN * M3;
      M3 += deltaM2 * deltaN * (count - 2) - 3 * deltaN * M2;
      M2 += deltaM2;
      M1 += deltaN;
    }

    @Override
    public void mergeAccumulator(MomentAccumulator<N> b) {
      long na = count;
      long nb = b.count;
      if (nb == 0) {
        return;
      }
      long n = count + b.count;
      if (n == 0) {
        return;
      }
      if(na == 0) {
        count = b.count;
        M1 = b.M1;
        M2 = b.M2;
        M3 = b.M3;
        M4 = b.M4;
        return;
      }
      
      double delta = b.M1 - M1;
      double delta2 = delta * delta;
      double delta3 = delta * delta * delta;
      double delta4 = delta * delta * delta * delta;
      
      
      // https://web.archive.org/web/20140423031833/http://people.xiph.org/~tterribe/notes/homs.html     
      double cM1 = (na * M1 + nb * b.M1) / n;
      double cM2 = M2 + b.M2 + (delta2 * na * nb ) / n; 
      double cM3 = M3 + b.M3 + (delta3 * na * nb * (na - nb)) / (n * n) + 3 * delta * (na * b.M2 - nb * M2) / n;
      double cM4 = M4 + b.M4 + (delta4 * na * nb * (na * na - na * nb + nb * nb)) / (n * n * n);
      cM4 += 6 * delta2 * (na * na * b.M2 + nb * nb * M2) / (n * n);
      cM4 += 4 * delta  * (na * b.M3 - nb * M3) / n;
      
      // Copy over the combined moments
      count = n;
      M1 = cM1;
      M2 = cM2;
      M3 = cM3;
      M4 = cM4;
    }

    @Override
    public Double extractOutput() {
      if (count == 0) {
        return Double.NaN;
      }
      if (type == MomentType.MEAN) {
        return M1;
      } else if (type == MomentType.VARIANCE) {
        return M2 / (count - 1);
      } else if (type == MomentType.SKEW) {
        return Math.sqrt(Math.floor(count)) * M3 / Math.pow(M2, 1.5);
      } else {
        return (Math.floor(count) * M4) / (M2 * M2) - 3;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(count, M1, M2, M3, M4);
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof MomentAccumulator)) {
        return false;
      }
      MomentAccumulator<?> otherAccum = (MomentAccumulator<?>) other;
      return (
        (count == otherAccum.count) &&
        this.doubleEquals(M1, otherAccum.M1, EPSILON) &&
        this.doubleEquals(M2, otherAccum.M2, EPSILON) &&
        this.doubleEquals(M3, otherAccum.M3, EPSILON) &&
        this.doubleEquals(M4, otherAccum.M4, EPSILON)
      );
    }

    @Override
    public String toString() {
      return (
        String.valueOf(count) +
        "," +
        String.valueOf(type) +
        "," +
        String.valueOf(M1) +
        "," +
        String.valueOf(M2) + 
        "," +
        String.valueOf(M3) +
        "," +
        String.valueOf(M4)
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
    private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

    @Override
    public void encode(MomentAccumulator<N> value, OutputStream outStream)
      throws CoderException, IOException {
      LONG_CODER.encode(value.count, outStream);
      STRING_CODER.encode(value.type.toString(), outStream);
      DOUBLE_CODER.encode(value.M1, outStream);
      DOUBLE_CODER.encode(value.M2, outStream);
      DOUBLE_CODER.encode(value.M3, outStream);
      DOUBLE_CODER.encode(value.M4, outStream);
    }

    @Override
    public MomentAccumulator<N> decode(InputStream inStream)
      throws CoderException, IOException {
      return new MomentAccumulator<>(
        LONG_CODER.decode(inStream),
        STRING_CODER.decode(inStream),
        DOUBLE_CODER.decode(inStream),
        DOUBLE_CODER.decode(inStream),
        DOUBLE_CODER.decode(inStream),
        DOUBLE_CODER.decode(inStream)
      );
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      LONG_CODER.verifyDeterministic();
      DOUBLE_CODER.verifyDeterministic();
      STRING_CODER.verifyDeterministic();
    }
  }
}
