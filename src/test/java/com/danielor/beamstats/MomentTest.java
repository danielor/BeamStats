package com.danielor.beamstats;

import static org.apache.beam.sdk.testing.CombineFnTester.testCombineFn;

import com.danielor.beamstats.Moment.MomentAccumulator;
import com.danielor.beamstats.Moment.MomentCoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.hamcrest.number.IsCloseTo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MomentTest {

  private static final Coder<MomentAccumulator<Number>> CODER = new MomentCoder<>();
  private static final List<MomentAccumulator<Number>> VALUES = Arrays.asList(
    new MomentAccumulator<>(1, Moment.MomentType.MEAN.toString(), 3.0, 4.0, 5.0, 6.0),
    new MomentAccumulator<>(2, Moment.MomentType.VARIANCE.toString(), 4.0, 5.0, 6.0, 7.0),
    new MomentAccumulator<>(3, Moment.MomentType.SKEW.toString(), 5.0, 6.0, 7.0, 8.0)
  );

  @Test
  public void testVarianceAccumulatorEncodeDecode() throws Exception {
    for (MomentAccumulator<Number> value : VALUES) {
      CoderProperties.coderDecodeEncodeEqual(CODER, value);
    }
  }

  @Test
  public void testVarianceAccumulatorSerializable() throws Exception {
    CoderProperties.coderSerializable(CODER);
  }
  
  @Test
  public void testMean() throws Exception {
    ArrayList<Number> list = new ArrayList<Number>(
      Arrays.asList(1, 2, 3, 4, 5)
    );
    testCombineFn(Moment.of(Moment.MomentType.MEAN), list, IsCloseTo.closeTo(3.0, 1e-7));
  }

  @Test
  public void testVariance() throws Exception {
    ArrayList<Number> list = new ArrayList<Number>(
      Arrays.asList(1, 2, 3, 4, 5)
    );
    testCombineFn(Moment.of(Moment.MomentType.VARIANCE), list, IsCloseTo.closeTo(2.5, 1e-7));
  }
  
  @Test
  public void testSkew() throws Exception {
    ArrayList<Number> list = new ArrayList<Number>(
      Arrays.asList(1, 2, 3, 4, 5)
    );
    testCombineFn(Moment.of(Moment.MomentType.SKEW), list, IsCloseTo.closeTo(0.0, 1e-7));
  }
  
  @Test
  public void testKurtosis() throws Exception {
    ArrayList<Number> list = new ArrayList<Number>(
      Arrays.asList(1, 2, 3, 4, 5)
    );
    testCombineFn(Moment.of(Moment.MomentType.KURTOSIS), list, IsCloseTo.closeTo(-1.3, 1e-7));
  }
}
