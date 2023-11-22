import os
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import Optional

import polars as pl
from box import Box
from icecream import ic
from polars import DataFrame, col

from fbi_crime.config import settings


class RecordType(StrEnum):
    BH = "BH"
    IR = "IR"


def passthrough(value: str) -> str:
    return value


def parse_int(value: str) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except:
        ic(value)
        return None


def parse_bool(value: str) -> Optional[bool]:
    if value is None:
        return None
    if value == "Y":
        return True
    elif value == "N":
        return False
    else:
        ic(value)
        return None


def parse_date(value: str) -> Optional[date]:
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except:
        ic(value)
        return None


def parse_record(line: str, schema: Box) -> tuple[RecordType, dict]:
    parsers = {
        "str": passthrough,
        "int": parse_int,
        "bool": parse_bool,
        "date": parse_date,
    }

    type = RecordType(line[0:2])
    assert type == RecordType.BH or type == RecordType.IR

    data = dict()
    for variable in schema[type]:
        if variable.drop:
            continue
        value = line[variable.start : variable.end + 1].strip()
        if value == "":
            value = None

        value = parsers[variable.type](value)
        data[variable.name] = value

    return type, data


def parse_file(file: str, schema: Box) -> dict:
    ic(file)

    with open(file) as f:
        lines = f.readlines()

    data = {"BH": [], "IR": []}
    for line in lines:
        if line.strip() == "":
            continue
        type, record = parse_record(line, schema)
        data[type].append(record)

    return data


def create_dataframe(data: list[dict], type: RecordType, schema: Box) -> DataFrame:
    types = {
        "str": pl.Utf8,
        "int": pl.Int32,
        "bool": pl.Boolean,
        "date": pl.Date,
    }

    data_columns = {}
    for record in data:
        for key, value in record.items():
            data_columns.setdefault(key, []).append(value)

    return DataFrame(
        pl.Series(
            variable.name, data_columns[variable.name], dtype=types[variable.type]
        )
        for variable in schema[type]
        if variable.drop is False
    )


def categorize_dataframe(
    df: DataFrame, record_type: RecordType, schema: Box
) -> DataFrame:
    expressions = [
        col(variable.name)
        .map_dict(variable["values"].to_dict(), default="Unknown (not mapped)")
        .cast(pl.Categorical)
        # .alias(variable.name + "_label")
        for variable in schema[record_type]
        if variable.drop is False and variable.categorical is True
    ]
    return df.with_columns(*expressions)


def main():
    dataframes = {"BH": [], "IR": []}
    schema = Box.from_yaml(filename=settings.data.layout)

    for year in range(2015, 2023):
        data = parse_file(f"data/{year}_HC_NATIONAL_MASTER_FILE_ENC.txt", schema)
        dataframes["BH"].append(create_dataframe(data["BH"], RecordType.BH, schema))
        dataframes["IR"].append(create_dataframe(data["IR"], RecordType.IR, schema))

    bh = pl.concat(dataframes["BH"])
    ir = pl.concat(dataframes["IR"])

    bh = categorize_dataframe(bh, RecordType.BH, schema)
    ir = categorize_dataframe(ir, RecordType.IR, schema)

    if os.path.exists("data/hatecrime-bh.arrow"):
        os.remove("data/hatecrime-bh.arrow")

    if os.path.exists("data/hatecrime-ir.arrow"):
        os.remove("data/hatecrime-ir.arrow")

    bh.write_ipc("data/hatecrime-bh.arrow")
    ir.write_ipc("data/hatecrime-ir.arrow")

    print("BH:", bh.shape)
    print("IR:", ir.shape)


if __name__ == "__main__":
    main()
