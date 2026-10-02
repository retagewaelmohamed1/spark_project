import pytest
from pyspark.sql import SparkSession
from pyspark_job import clean_data

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("PySpark-CI-Testing") \
        .getOrCreate()

def test_clean_data(spark):
    input_data = [
        ("Alice", 100.0),   # سجل صحيح
        ("Bob", -50.0),     # amount <= 0
        (None, 200.0),      # name is NULL
        ("Charlie", 0.0)    # amount <= 0
    ]
    schema = ["name", "amount"]
    
    df = spark.createDataFrame(input_data, schema)
    result_df = clean_data(df)
    results = result_df.collect()
    
    # 1. التأكد أن السجل المقبول هو Alice فقط
    assert len(results) == 1
    assert results[0]["name"] == "Alice"
    
    # 2. التأكد من حساب الضريبة صح (100 * 1.20 = 120.0)
    assert pytest.approx(results[0]["amount_with_tax"], 0.01) == 120.0
