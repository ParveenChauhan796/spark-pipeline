"""Pure transformations (no I/O): column expressions and DataFrame -> DataFrame functions.

Everything here uses built-in Spark functions only. No Python UDFs: a UDF would ship every row to a
Python process and is opaque to Spark's optimizer, while built-ins run inside the JVM and are
optimized together with the rest of the query plan.
"""
