import re
from datetime import datetime

import pandas as pd


def dot_remover(string: str) -> str:
    """Remove dots from entity ids."""

    return re.sub(r"[^0-9]", "", string)


def date_formatter(string):
    """Converts from "%d-%m-%Y" to "%Y-%m-%d"."""

    # Note: It is not possible to use df.to_datetime because some dates are
    # before 1970-01-01.

    if pd.isna(string):
        return None

    old_date = datetime.strptime(string, "%d-%m-%Y")
    new_date = old_date.strftime("%Y-%m-%d")
    return new_date


def df_splitter(
    df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split data according to their three base entities. Also correct the
    date if present."""

    if "date_striking_off" in df.columns:
        df["date_striking_off"] = (
            df["date_striking_off"]
            .apply(lambda x: date_formatter(x))
        )

    ent = df[df.iloc[:, 0].str.startswith(("0", "1"))].reset_index(drop=True)
    est = df[df.iloc[:, 0].str.startswith("2")].reset_index(drop=True)
    bra = df[df.iloc[:, 0].str.startswith("9")].reset_index(drop=True)
    return ent, est, bra
