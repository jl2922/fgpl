# FGPL
Fast Generic Parallel Library

[![Build Status](https://travis-ci.org/jl2922/fgpl.svg?branch=master)](https://travis-ci.org/jl2922/fgpl)

## Usage
TBA

## Benchmark
![Serialize and Parse Time](https://github.com/jl2922/fgpl/blob/master/performance.png)

Spark's implementations are from https://spark.apache.org/examples.html.
The Spark cluster is set up using AWS EMR's default settings.

FGPL's implementations are in the DistHashMapTest and DistRangeTest of CI.

The input file for word count is from Shakespeare and the Bible, repeated ~100 times to make it ~1GB in size.
We use MapReduce to count the number of occurrences of each word.
