package com.danielor.beamstats;

import static org.apache.beam.sdk.testing.CombineFnTester.testCombineFn;

import com.danielor.beamstats.SimpleRegression.SimpleRegressionAccumulator;
import com.danielor.beamstats.SimpleRegression.SimpleRegressionCoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.values.Row;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SimpleRegressionTest {
  private static final double EPSILON = 1e-7; // For floating point comparisons;
  
  public class RegressionMatcher extends TypeSafeMatcher<Row> {
    Row output;
    
    RegressionMatcher(Row o) {
      output = o;
    }
    
    private boolean doubleEquals(double f1, double f2, double epsilon) {
      return Math.abs(f1 - f2) < epsilon;
    }
    
    
    @Override
    public void describeTo(Description description) {
      description.appendText(" regression matcher");
      
    }

    @Override
    protected boolean matchesSafely(Row item) {
      try {
        double expectedAlpha = output.getValue("alpha");
        double expectedBeta = output.getValue("beta");
        double expectedCorrelation = output.getValue("correlation");
        double itemAlpha = item.getValue("alpha");
        double itemBeta = item.getValue("beta");
        double itemCorrelation = item.getValue("correlation");
        return doubleEquals(expectedAlpha, itemAlpha, EPSILON) &&
            doubleEquals(expectedBeta, itemBeta, EPSILON) &&
            doubleEquals(expectedCorrelation, itemCorrelation, EPSILON);

      } catch(Exception e) {
        return false;
      }
    }
    
  }

  private static final Coder<SimpleRegressionAccumulator> CODER = new SimpleRegressionCoder();
  private static final List<SimpleRegressionAccumulator> VALUES = Arrays.asList(
      new SimpleRegressionAccumulator(2, 3.0, 4.0, 5.0, 6.0, 7.0),
      new SimpleRegressionAccumulator(3, 4.0, 5.0, 6.0, 7.0, 8.0)
  );
  
  @Test
  public void testSimpleRegressionAccumulatorEncodeDecode() throws Exception {
    for (SimpleRegressionAccumulator value : VALUES) {
      CoderProperties.coderDecodeEncodeEqual(CODER, value);
    }
  }

  @Test
  public void testSimpleRegressionAccumulatorSerializable() throws Exception {
    CoderProperties.coderSerializable(CODER);
  }
  
  @Test
  public void testRegressionOnLine() {
    ArrayList<Row> list = new ArrayList<Row>(Arrays.asList(
        Row.withSchema(SimpleRegression.point2dSchema).addValue(1.0).addValue(1.0).build(),
        Row.withSchema(SimpleRegression.point2dSchema).addValue(2.0).addValue(2.0).build(),
        Row.withSchema(SimpleRegression.point2dSchema).addValue(3.0).addValue(3.0).build(),
        Row.withSchema(SimpleRegression.point2dSchema).addValue(4.0).addValue(4.0).build(),
        Row.withSchema(SimpleRegression.point2dSchema).addValue(5.0).addValue(5.0).build()
    ));
    
    Row result = Row.
        withSchema(SimpleRegression.outputSchema)
        .addValue(0.0)
        .addValue(1.0)
        .addValue(1.0)
        .build();
    RegressionMatcher matcher = new RegressionMatcher(result);
    testCombineFn(SimpleRegression.of(), list, matcher);
    
  }

}
