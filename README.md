# BeamStats
BeamStats is a collection of statistical transforms for the Apache Beam framework written in Java.

## Usage

You can calculate the moments of a distribution by using the Moment object.

```java
import com.danielor.beamstats.Moment;

PCollection<Double> input = buildInput();
PCollection<Double> mean = input.apply(Moment.globally(Moment.MomentType.MEAN));
PCollection<Double> variance = input.apply(Moment.globally(Moment.MomentType.VARIANCE));
PCollection<Double> skew = input.apply(Moment.globally(Moment.MomentType.SKEW));
PCollection<Double> kurtosis = input.apply(Moment.globally(Moment.MomentType.KURTOSIS));
```

You can calculate a simple linear regression using the SimpleRegression object.

```java
import com.danielor.beamstats.SimpleRegression;

// The schema for the row needs to be equal to SimpleRegression.point2dSchema
PCollection<Row> input = buildInput();

// The schema for the simple regression row can be found at SimpleRegression.outputSchema
PCollection<Row> regression = input.apply(SimpleRegression.globally());

```
