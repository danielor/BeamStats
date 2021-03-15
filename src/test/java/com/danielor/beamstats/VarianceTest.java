package com.danielor.beamstats;

import static org.apache.beam.sdk.testing.CombineFnTester.testCombineFn;

import com.danielor.beamstats.Variance.VarianceAccumulator;
import com.danielor.beamstats.Variance.VarianceCoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class VarianceTest {

  private static final Coder<VarianceAccumulator<Number>> CODER = new VarianceCoder<>();
  private static final List<VarianceAccumulator<Number>> VALUES = Arrays.asList(
    new VarianceAccumulator<>(1, 3.0, 4.0),
    new VarianceAccumulator<>(2, 4.0, 5.0),
    new VarianceAccumulator<>(3, 5.0, 6.0)
  );

  @Test
  public void testVarianceAccumulatorEncodeDecode() throws Exception {
    for (VarianceAccumulator<Number> value : VALUES) {
      CoderProperties.coderDecodeEncodeEqual(CODER, value);
    }
  }

  @Test
  public void testVarianceAccumulatorSerializable() throws Exception {
    CoderProperties.coderSerializable(CODER);
  }

  @Test
  public void testVariance() throws Exception {
    ArrayList<Number> list = new ArrayList<Number>(
      Arrays.asList(1, 2, 3, 4, 5)
    );
    testCombineFn(Variance.of(), list, 2.0);
  }
}
