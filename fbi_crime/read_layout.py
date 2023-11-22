import camelot
import polars as pl
import srsly
from polars import col, lit, when

for year in (2015, 2021):
    layout = f"layouts/{year} Hate Crime Master File Record Description.pdf"
    tables = camelot.read_pdf(layout, pages="6-end", flavor="lattice")

    dataframes = []
    for table in tables:
        df = table.df.copy()
        df.columns = [
            name.lower().replace("  ", " ").replace(" ", "_").replace("\n", "")
            for name in df.iloc[0]
        ]
        df = df[1:]
        dataframes.append(pl.from_pandas(df))

    combined = pl.concat(dataframes)
    out = (
        combined.filter((col("record_position") != "") & (col("data_length") != ""))
        .with_columns(
            col("description")
            .str.to_lowercase()
            .str.replace_all("  ", " ")
            .str.replace_all(" ", "_")
            .str.replace_all("-", "_")
            .str.replace_all("\(", "")
            .str.replace_all("\)", "")
            .str.replace_all("#", "")
            .alias("name"),
            col("record_position").str.split("-").alias("positions"),
            col("data_length").map_elements(list).alias("lengths"),
        )
        .with_columns(
            col("positions")
            .map_elements(lambda s: int(s[0]) - 1)
            .cast(pl.Int32)
            .alias("start"),
            col("positions")
            .map_elements(lambda s: int(s[::-1][0]) - 1)
            .cast(pl.Int32)
            .alias("end"),
            col("lengths").map_elements(lambda s: s[0]).alias("type_code"),
            col("lengths")
            .map_elements(lambda s: int("".join(s[1:])))
            .cast(pl.Int32)
            .alias("length"),
        )
        .with_columns(
            when(col("type_code") == "A")
            .then(lit("string"))
            .when(col("type_code") == "N")
            .then(lit("integer"))
            .otherwise(lit("unknown"))
            .alias("type")
        )
        .select(
            "name",
            "type",
            "start",
            "end",
            "length",
        )
    )
    srsly.write_yaml(f"layouts/{year}-layout-generated.yaml", out.to_dicts())
