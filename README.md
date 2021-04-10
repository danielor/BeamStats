# BeamStats
BeamStats is a collection of statistical transforms for the Apache Beam framework written in Java.

## Usage

You can calculate the moments of a distribution by using the Moment object.

```java
import com.danielor.beamstats.Moment

PCollection<Double> input = buildInput();
PCollection<Double> mean = input.apply(Moment.globally(Moment.MomentType.MEAN));
PCollection<Double> variance = input.apply(Moment.globally(Moment.MomentType.VARIANCE));
```
