def test_delta_write_append_read_and_history(spark, tmp_path):
    path = str(tmp_path / "smoke_delta")
    df = spark.createDataFrame([(1, "a"), (2, "b")], "id int, label string")

    df.write.format("delta").save(path)  # version 0
    df.write.format("delta").mode("append").save(path)  # version 1

    assert spark.read.format("delta").load(path).count() == 4

    history = spark.sql(f"DESCRIBE HISTORY delta.`{path}`")
    assert history.count() == 2
    assert sorted(r["version"] for r in history.collect()) == [0, 1]
