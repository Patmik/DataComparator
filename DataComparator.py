import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when
from pyspark.sql import DataFrame
from datetime import datetime
import pandas as pd
import os
from pyspark.sql.functions import lit, array, explode, struct



class DataComparator:
    def __init__(self, df1, df2, primary_key,spark):
        self.df1 = self._to_spark_df(spark,df1)
        self.df2 = self._to_spark_df(spark,df2)
        self.primary_key = primary_key
            
    def _to_spark_df(self, spark, data):
        if isinstance(data, DataFrame):
            return data
    
        elif isinstance(data, pd.DataFrame):
            data.to_csv("convertion.csv", index=False)
            #return print(f"{data}.csv")
            return spark.read.csv("convertion.csv", header=True, inferSchema=True)
    
        elif isinstance(data, str) and os.path.exists(data) and data.lower().endswith(".csv"):
            return spark.read.csv(data, header=True, inferSchema=True)
        else:
            raise TypeError(
                f"Unsupported data type: {type(data)}. "
                f"Supports: Spark DF, Pandas DF, CSV (path)."
            )
            
    def _compare_schema(self):
        schema1 = {(x.name,x.dataType.simpleString()) for x in self.df1.schema.fields}
        schema2 = {(x.name,x.dataType.simpleString()) for x in self.df2.schema.fields}

        missing_in_df1 = schema2 - schema1
        missing_in_df2 = schema1 - schema2

        return {'missing_in_df1': list(missing_in_df1),
                'missing_in_df2': list(missing_in_df2),
                'identical' : schema1==schema2 }
    
    def _compare_row_count(self):
        quantity_df1 = self.df1.count()
        quantity_df2 = self.df2.count()
        return {'quantity_df1' : quantity_df1,
                'quantity_df2' : quantity_df2,
                'difference' : quantity_df1 - quantity_df2}
    
    def _compare_values(self):
        common_cols = [c for c in self.df1.columns if c in self.df2.columns and c not in self.primary_key]

        joined = (self.df1.alias('a').join(self.df2.alias('b'), on = self.primary_key, how = 'outer'))

        differences = []
        for c in common_cols:
            differences.append(
                when(col(f'a.{c}').isNull() & col(f'b.{c}').isNotNull(), True)
                .when(col(f'a.{c}').isNotNull() & col(f'b.{c}').isNull(), True)
                .when(col(f'a.{c}') != col(f'b.{c}'),True)
                .otherwise(False).alias(f'difference_{c}')         
            )

        joined_with_differences = joined.select(
            *self.primary_key,
            *[col(f'a.{df1_c}').alias(f'{df1_c}_df1') for df1_c in common_cols],
            *[col(f'b.{df2_c}').alias(f'{df2_c}_df2') for df2_c in common_cols],
            *differences
        )

        melted = joined_with_differences.select(
            *self.primary_key,
            F.explode(
                F.array([
                    F.when(F.col(f"difference_{c}") == True,
                        F.struct(
                            F.lit(c).alias("column"),
                            F.col(f"{c}_df1").cast("string").alias("value_in_df1"),
                            F.col(f"{c}_df2").cast("string").alias("value_in_df2"),
                            F.when(F.col(f"{c}_df1").isNull() & F.col(f"{c}_df2").isNotNull(), F.lit("added"))
                            .when(F.col(f"{c}_df1").isNotNull() & F.col(f"{c}_df2").isNull(), F.lit("removed"))
                            .otherwise(F.lit("changed")).alias("difference_type")
                        )
                    )
                    for c in common_cols
                ])
            ).alias("diff")
        ).select(
            *self.primary_key,
            F.col("diff.column"),
            F.col("diff.value_in_df1"),
            F.col("diff.value_in_df2"),
            F.col("diff.difference_type")
        ).filter(F.col("column").isNotNull())

        return melted
    
    def summarize(self,logger = False):
        schema_difference = self._compare_schema()
        row_difference = self._compare_row_count()
        value_difference = self._compare_values()._jdf.showString(self._compare_values().count(), int(False), False)

        message = (
            "SCHEMA DIFFERENCES \n"
            f"Missing columns in df1: {schema_difference['missing_in_df1']} \n"
            f"Missing columns in df2: {schema_difference['missing_in_df2']} \n"
            f"Identical: {schema_difference['identical']} \n"
            "\n"
            "ROW NUMBER DIFFERENCES \n"
            f"Number of rows in df1: {row_difference['quantity_df1']} \n"
            f"Number of rows in df2: {row_difference['quantity_df2']} \n"
            f"Difference: {row_difference['difference']} \n"
            "\n"
            "VALUES DIFFERENCES \n"
        )
        if logger == True:
            current_datetime = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
            str_current_datetime = str(current_datetime)
            file_name = str_current_datetime+".txt"
            file = open(file_name, 'w')
            with file as file_to_write:
                for line in message:
                    file_to_write.write(line)
                file_to_write.write(value_difference)
            file.close()
            
        return print(message,value_difference)
        
