from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as _sum, round

def create_aggregates(
    df: DataFrame, 
    group_by_cols: list, 
    agg_col: str, 
    alias_col: str = "total_value",
    round_places: int = 2
) -> DataFrame:
    """
    Generic aggregation function.
    """
    agged = df.groupBy(*group_by_cols).agg(round(_sum(agg_col), round_places).alias(alias_col))
    return agged.orderBy(*group_by_cols)


