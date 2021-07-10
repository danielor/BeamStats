package com.danielor.beamstats;

import static org.apache.beam.sdk.testing.CombineFnTester.testCombineFn;

import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Test;

public class OrderStatisticTest {

  @Test
  public void testKthLargest() {
    ArrayList<Number> list = new ArrayList<Number>(
      Arrays.asList(1, 2, 3, 4, 5)
    );
    Integer result = 4;
    testCombineFn(
      OrderStatistic.of(OrderStatistic.OrderType.LARGEST, 2),
      list,
      result
    );
  }

  @Test
  public void testKthSmallest() {
    ArrayList<Number> list = new ArrayList<Number>(
      Arrays.asList(1, 2, 3, 4, 5)
    );
    Integer result = 2;
    testCombineFn(
      OrderStatistic.of(OrderStatistic.OrderType.SMALLEST, 2),
      list,
      result
    );
  }
}
