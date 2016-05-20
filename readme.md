# Process CSV datasets efficiently

This is just a collection of functions to process
CSV (and other) data structures. The functions are
designed in such a way that they can be combined
to build processing pipelines. All functions return
`Generator`s to ensure that big data sets can be
processed without having everything in memory.

The concepts are inspired by lazy functional
programming which is very cool :-)

