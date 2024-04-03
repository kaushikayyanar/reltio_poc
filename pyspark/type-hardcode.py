import findspark
findspark.init()
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, create_map, lit, struct

def find_non_null_value_keys(json_data):
    non_null_keys = {"single_attributes": []}
    for key, value in json_data["attributes"].items():
        if isinstance(value, list) and value:
            nested_value = value[0].get("value", None)
            if isinstance(nested_value, dict) and any(v is not None for v in nested_value.values()):
                non_null_keys[key] = list(nested_value.keys())
            else:
                non_null_keys["single_attributes"].append(key)
    return non_null_keys

def process_basic_details_df(basic_details_df,schema):

    columns = schema['single_attributes']
    key_value_pairs = [
        (lit(column),
         collect_list(struct(lit("value").alias("value"),
                             col(column).cast("string").alias("value"))))
        for column in columns
    ]

    json_df = basic_details_df.select(create_map(*[item for sublist in key_value_pairs for item in sublist]).alias("attributes"))
    json_string_basic = json_df.toJSON().collect()[0]

    basic_fin = json.loads(json_string_basic)

    return basic_fin

def process_data_df(data_df, name, schema):

    json_objects = []
    for row in data_df.collect():
        json_obj = {}
        for column in schema[name]:
            json_obj[column] = [{"value": str(row[column])}]
        json_objects.append({"value": json_obj})

    json_output = {name: json_objects}

    return json_output


def process_combined_data(basic_details_df, schema, *dfs):

    combined_data = []

    basic_details_df.createOrReplaceTempView("basic_details")

    for id_row in spark.sql("SELECT DISTINCT id FROM basic_details").collect():
        id_value = id_row["id"]
        individual_data = {"type": "configuration/entityTypes/Customer"}
        filtered_basic_df = spark.sql(f"SELECT * FROM basic_details WHERE id = '{id_value}'")
        individual_data.update(process_basic_details_df(filtered_basic_df,schema))
        for df in dfs:
            df.createOrReplaceTempView("temp_df")
            name = next(name for name, data in schema.items() if data == df.columns)
            result = spark.sql(f"SELECT * FROM temp_df WHERE id = '{id_value}'")
            finresult = process_data_df(result, name, schema)
            individual_data["attributes"].update(finresult)

        combined_data.append(individual_data)

    return combined_data

spark = SparkSession.builder \
    .appName("test") \
    .getOrCreate()

basic_path = "C:/reltio pyspark/file/basics copy.csv"
education_path = "C:/reltio pyspark/file/education copy.csv"
address_path = "C:/reltio pyspark/file/address copy.csv"
phone_path = "C:/reltio pyspark/file/phone.csv"

basic_details_df = spark.read.csv(basic_path, header=True, inferSchema=True)
education_details_df = spark.read.csv(education_path, header=True, inferSchema=True)
address_details_df = spark.read.csv(address_path, header=True, inferSchema=True)
phone_details_df = spark.read.csv(phone_path, header=True, inferSchema=True)

json_file_path = "C:/reltio pyspark/file/file1.json"
with open(json_file_path, "r") as file:
    json_data = json.load(file)

schema = find_non_null_value_keys(json_data)
print(schema)

result = process_combined_data(basic_details_df, schema, education_details_df, address_details_df, phone_details_df)

with open(r"C:\reltio pyspark\file\op.txt", "w") as f:
    json.dump(result, f, indent=4)

print(json.dumps(result, indent=4))

spark.stop()
