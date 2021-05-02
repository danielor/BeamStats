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
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn.Accumulator;
import org.apache.beam.sdk.values.Row;

/**
 * This class calculates the simple linear regression of 2d sample points.
 * https://en.wikipedia.org/wiki/Simple_linear_regression
 */
public class SimpleRegression {
  
  private static final double EPSILON = 1e-7; // For floating point comparisons;

  
  /**
   * An apache beam schema for (x, y) points. 
   */
  public final static Schema point2dSchema = Schema
      .builder()
      .addDoubleField("x")
      .addDoubleField("y")
      .build();
  
  /**
   * An apache beam schema for the output of a regression.
   * alpha = y intercept
   * beta = slope
   * correlation = correlation coefficient
   */
  public final static Schema outputSchema = Schema
      .builder()
      .addDoubleField("alpha")
      .addDoubleField("beta")
      .addDoubleField("correlation")
      .build();
  
  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<N>} and returns a
   * {@code PCollection<Double>} whose content is the simple regression of the input
   *
   * @return
   */
  public static Combine.Globally<Row, Row> globally() {
    return Combine.globally(SimpleRegression.of());
  }

  /**
   * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K,Row>>} and returns
   * a {@code PCollection<KV<K, Row>>} that computes the simple regression for each key K.
   *
   * @param <K> The type of keys
   * @return
   */
  public static <K> Combine.PerKey<K, Row, Row> perKey() {
    return Combine.perKey(SimpleRegression.of());
  }

  /**
   * Creates a {@code Combine.CombineFn} that computes the variance of a {@code PCollection}
   *
   * @param type The moment type (mean, variance, skew, etc.)
   * @param <N> the type of {@code Number}
   * @return
   */
  public static Combine.AccumulatingCombineFn<Row, SimpleRegressionAccumulator, Row> of() {
    return new SimpleRegressionFunction();
  }
  
  /**
   * The simple regression function creates the accumulator and coder needed by the Apache Beam combine operator
   */
  @SuppressWarnings("serial")
  private static class SimpleRegressionFunction extends Combine.AccumulatingCombineFn<Row, SimpleRegressionAccumulator, Row> {

    @Override
    public SimpleRegressionAccumulator createAccumulator() {
      return new SimpleRegressionAccumulator();
    }
    
    @Override
    public Coder<SimpleRegressionAccumulator> getAccumulatorCoder(
      CoderRegistry registry,
      @SuppressWarnings("rawtypes") Coder inputCoder
    ) {
      return new SimpleRegressionCoder();
    }
    
  }
 
  /**
   * The accumulator that calculates the regression line using the ordinary least square approach.
   */
  static class SimpleRegressionAccumulator implements Accumulator<Row, SimpleRegressionAccumulator, Row> {
    long count = 0;
    double M1x = 0.0;
    double M1y = 0.0;
    double M2x = 0.0;
    double M2y = 0.0;
    double Sxy = 0.0;
    
    SimpleRegressionAccumulator() {
      this(0, 0.0, 0.0, 0.0, 0.0, 0.0);
    }
    
    SimpleRegressionAccumulator(long c, double m1x, double m1y, double m2x, double m2y, double sxy) {
      count = c;
      M1x = m1x;
      M1y = m1y;
      M2x = m2x;
      M2y = m2y;
      Sxy = sxy;
    }
    
    private boolean doubleEquals(double f1, double f2, double epsilon) {
      return Math.abs(f1 - f2) < epsilon;
    }
    
    @Override
    public int hashCode() {
      return Objects.hash(count, M1x, M1y, M2x, M2y, Sxy);
    }
    
    @Override
    public boolean equals(Object other) {
      if (!(other instanceof SimpleRegressionAccumulator)) {
        return false;
      }
      SimpleRegressionAccumulator otherAccum = (SimpleRegressionAccumulator) other;
      return (
        count == otherAccum.count &&
        doubleEquals(M1x, otherAccum.M1x, EPSILON) &&
        doubleEquals(M1y, otherAccum.M1y, EPSILON) &&
        doubleEquals(M2x, otherAccum.M2x, EPSILON) &&
        doubleEquals(M2y, otherAccum.M2y, EPSILON) &&
        doubleEquals(Sxy, otherAccum.Sxy, EPSILON)
      );
    }

    @Override
    public void addInput(Row input) {
      double x = input.getDouble("x");
      double y = input.getDouble("y");
      long n = count;
      count += 1;
      Sxy += (M1x - x) * (M1y - y) * n /(n + 1);
      
      double deltax = x - M1x;
      double deltaxN = deltax/ count;
      double deltaxM2 = deltax * deltaxN * n;
      M2x += deltaxM2;
      M1x += deltaxN;
      
      double deltay = y - M1y;
      double deltayN = deltay / count;
      double deltayM2 = deltay * deltayN * n;
      M2y += deltayM2;
      M1y += deltayN;      
    }
    
    @Override
    public void mergeAccumulator(SimpleRegressionAccumulator b) {
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
        M1x = b.M1x;
        M2x = b.M2x;
        M1y = b.M1y;
        M2y = b.M2y;
        Sxy = b.Sxy;
      }
      
      double deltaX = b.M1x - M1x;
      double deltaX2 = deltaX * deltaX;
      double cM1x = (na * M1x + nb * b.M1x) / n;
      double cM2x = M2x + b.M2x + (deltaX2 * na * nb) / n;
      
      double deltaY = b.M1y - M1y;
      double deltaY2 = deltaY * deltaY;
      double cM1y = (na * M1y + nb * b.M1y) / n;
      double cM2y = M2y + b.M2y + (deltaY2 * na * nb) / n;
      
      double cSxy = Sxy + b.Sxy + na * nb * deltaX * deltaY  / n;
      
      M1x = cM1x;
      M2x = cM2x;
      M1y = cM1y;
      M2y = cM2y;
      Sxy = cSxy;
      count = n;      
    }
    

    @Override
    public String toString() {
      return (
        String.valueOf(count) +
        "," +
        String.valueOf(M1x) +
        "," +
        String.valueOf(M1y) +
        "," +
        String.valueOf(M2x) + 
        "," +
        String.valueOf(M2y) +
        "," +
        String.valueOf(Sxy)
      );
    }

    @Override
    public Row extractOutput() {
      if (count < 2) {
        return Row
            .withSchema(outputSchema)
            .addValue(Double.NaN)
            .addValue(Double.NaN)
            .addValue(Double.NaN)
            .build();
      }
      
      double beta = Sxy / M2x;
      double alpha = M1y - beta * M1x;
      
      double xStd = Math.sqrt(M2x / (count - 1));
      double yStd = Math.sqrt(M2y / (count - 1));
      double correlation = Sxy / ((count -1) * xStd * yStd);
      return Row
          .withSchema(outputSchema)
          .addValue(alpha)
          .addValue(beta)
          .addValue(correlation)
          .build();
    }
  }
  
  /**
   * A coder used tot transform the accumulator for Apache Beam.
   */
  @SuppressWarnings("serial")
  static class SimpleRegressionCoder extends AtomicCoder<SimpleRegressionAccumulator> {

    private static final Coder<Long> LONG_CODER = BigEndianLongCoder.of();
    private static final Coder<Double> DOUBLE_CODER = DoubleCoder.of();
    
    @Override
    public void encode(SimpleRegressionAccumulator value, OutputStream outStream) throws CoderException, IOException {
      LONG_CODER.encode(value.count, outStream);
      DOUBLE_CODER.encode(value.M1x, outStream);
      DOUBLE_CODER.encode(value.M1y, outStream);
      DOUBLE_CODER.encode(value.M2x, outStream);
      DOUBLE_CODER.encode(value.M2y, outStream);
      DOUBLE_CODER.encode(value.Sxy, outStream);
    }

    @Override
    public SimpleRegressionAccumulator decode(InputStream inStream) throws CoderException, IOException {
      return new SimpleRegressionAccumulator(
          LONG_CODER.decode(inStream),
          DOUBLE_CODER.decode(inStream),
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
    }
    
  }
}
