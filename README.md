# BeamStats
BeamStats is a collection of statistical transforms for the Apache Beam framework written in Java.

## Usage


```java
PCollection<Double> input = buildInput();
PCollection<Double> variance = input.apply(Variance.globally());
```
