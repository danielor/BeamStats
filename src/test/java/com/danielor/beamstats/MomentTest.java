package com.danielor.beamstats;

import static org.apache.beam.sdk.testing.CombineFnTester.testCombineFn;

import com.danielor.beamstats.Moment.MomentAccumulator;
import com.danielor.beamstats.Moment.MomentCoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MomentTest {

  private static final Coder<MomentAccumulator<Number>> CODER = new MomentCoder<>();
  private static final List<MomentAccumulator<Number>> VALUES = Arrays.asList(
    new MomentAccumulator<>(1, 3.0, 4.0),
    new MomentAccumulator<>(2, 4.0, 5.0),
    new MomentAccumulator<>(3, 5.0, 6.0)
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
    testCombineFn(Moment.of(Moment.MomentType.MEAN), list, 3.0);
  }

  @Test
  public void testVariance() throws Exception {
    ArrayList<Number> list = new ArrayList<Number>(
      Arrays.asList(1, 2, 3, 4, 5)
    );
    testCombineFn(Moment.of(Moment.MomentType.VARIANCE), list, 2.0);
  }
}
