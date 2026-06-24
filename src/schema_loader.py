import json
# pyrefly: ignore [missing-import]
from pyspark.sql.types import *


def build_schema(field_list):
    fields = []

    for field in field_list:

        name = field["name"]
        nullable = field.get("nullable", True)

        field_type = field["type"]

        if isinstance(field_type, dict):

            nested_fields = build_schema(field_type["fields"])

            spark_type = StructType(nested_fields)

        else:

            type_mapping = {
                "string": StringType(),
                "double": DoubleType(),
                "integer": IntegerType(),
                "long": LongType(),
                "boolean": BooleanType()
            }

            spark_type = type_mapping[field_type]

        fields.append(
            StructField(name, spark_type, nullable)
        )

    return fields


def load_schema(schema_path):

    with open(schema_path, "r") as f:
        schema_json = json.load(f)

    return StructType(
        build_schema(schema_json["fields"])
    )