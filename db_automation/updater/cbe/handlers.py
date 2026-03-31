import os
import shutil

import pandas as pd
from sqlalchemy import text

import db_automation.updater.cbe.functions as fc
from db_automation.bot.zipbot import ZipBot
from db_automation.config import Config
from db_automation.database.classes import CbeDbConnector
from db_automation.logger.config import update_cbe_logger
from db_automation.updater.cbe.dictionaries import col_dict


def zipbot(date_) -> None:
    """Run zipbot and store result."""

    bot = ZipBot(date_)
    bot.save_to(dest=Config.ZIP_DESTINATION, filename=Config.ZIP_FILENAME)
    bot.open_zip(dest=Config.ZIP_DESTINATION, filename=Config.ZIP_FILENAME)


def preprocess_cbe_data(path: str) -> None:
    """Separate data per entity.

    Clean the data by removing dots from IDs, convert dates to the correct
    format, divide data per category (enterprise, establishment or branch),
    and do some minimal final checks.
    """

    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file() and entry.name.endswith(".csv"):
                file = entry.name.removesuffix(".csv")

                # Delete meta.csv
                if file == "meta":
                    # delete file
                    os.remove(entry.path)
                    continue

                df = pd.read_csv(entry.path, dtype="str")
                df.drop_duplicates(inplace=True)

                # Handle code.csv, which serves as a semantic file
                if file == "code":
                    df_dict: dict[str, pd.DataFrame] = {}

                    for category in df["Category"].unique():
                        cat = category.lower()
                        df_dict[cat] = (
                            df[df["Category"] == category]
                            .drop(columns="Category")
                            .reset_index(drop=True)
                        )

                    for cat, df in df_dict.items():
                        df.columns = col_dict["codes"]
                        df.to_csv(
                            os.path.join(path, f"{cat}.csv"),
                            index=False
                        )

                    os.remove(entry.path)
                    continue

                # Handle remaining files. Remove dot-format for all files.
                update_cbe_logger.info("Removing dot-format from IDs...")
                df.iloc[:, 0] = (
                    df.iloc[:, 0]
                    .apply(lambda x: fc.dot_remover(x))
                )

                # Case 1: establishment.csv or branch.csv
                if file in ["establishment", "branch"]:
                    update_cbe_logger.info(f"Handling {file}.csv...")
                    df.iloc[:, -1] = (
                        df.iloc[:, -1]
                        .apply(lambda x: fc.dot_remover(x))
                    )
                    df["StartDate"] = (
                        df["StartDate"]
                        .apply(lambda x: fc.date_formatter(x))
                    )
                    df.columns = (
                        col_dict['establishment']
                        if file == "establishment"
                        else col_dict['branch']
                    )
                    df.to_csv(entry.path, index=False)

                # Case 2: enterprise.csv
                elif file == "enterprise":
                    update_cbe_logger.info(f"Handling {file}.csv...")
                    df["StartDate"] = (
                        df["StartDate"]
                        .apply(lambda x: fc.date_formatter(x))
                    )

                    df.columns = col_dict['enterprise']
                    df.to_csv(entry.path, index=False)

                # Case 3: Other files
                else:
                    update_cbe_logger.info(f"Handling {file}.csv...")

                    # First assign columns, then split.
                    if file == "activity":
                        df.columns = col_dict["activity"]
                    elif file == "address":
                        df.columns = col_dict["address"]
                    elif file == "contact":
                        df.columns = col_dict["contact"]
                    elif file == "denomination":
                        df.columns = col_dict["denomination"]
                    else:
                        update_cbe_logger.warning(
                            f"Unknown file while assigning columns: {file}"
                        )

                    ent, est, bra = fc.df_splitter(df)

                    ent.to_csv(
                        os.path.join(path, f"{file}enterprise.csv"),
                        index=True  # is column record_id
                    )
                    est.to_csv(
                        os.path.join(path, f"{file}establishment.csv"),
                        index=True  # is column record_id
                    )
                    bra.to_csv(
                        os.path.join(path, f"{file}branch.csv"),
                        index=True  # is column record_id
                    )
                    os.remove(entry.path)


def truncate_db(db) -> dict:
    """Truncate database tables."""

    cbe_db = CbeDbConnector(db)
    table_trunc_dct: dict = vars(cbe_db).copy()
    del table_trunc_dct['url']
    del table_trunc_dct['engine']
    table_dct = table_trunc_dct.copy()

    update_cbe_logger.info("Start truncating tables...")

    trunc_stmt = "TRUNCATE TABLE {} RESTART IDENTITY CASCADE"

    with cbe_db.engine.begin() as conn:
        stmt_ent = (
            trunc_stmt.format(table_trunc_dct.pop("enterprise").__table__.name)
        )
        stmt_est = (
            trunc_stmt.format(table_trunc_dct.pop("establishment").__table__.name)
        )
        stmt_bra = (
            trunc_stmt.format(table_trunc_dct.pop("branch").__table__.name)
        )

        conn.execute(text(stmt_ent))
        conn.execute(text(stmt_est))
        conn.execute(text(stmt_bra))

        # Theoretically this code is redundant because the use of cascade in
        # the database. However, we keep it just to be sure.
        for table in table_trunc_dct.values():
            stmt = trunc_stmt.format(table.__table__.name)
            conn.execute(text(stmt))

        update_cbe_logger.info("Database truncated.")

    return table_dct


def populate_db(table_dct: dict, db) -> bool:
    """Populate database with new data."""

    success = False
    copy_stmt = """
        COPY {} FROM STDIN WITH (
        FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"', ENCODING 'UTF8')
    """

    conn = CbeDbConnector(db).engine.raw_connection()

    # Due to database relations, population has to be in correct order.
    # We use try/except/finally to assure the database connection is closed
    # i.c.o. failure.

    try:
        # Step 1: Enterprise
        update_cbe_logger.info("Populating table 'enterprise'.")
        cursor_ent = conn.cursor()

        with open(
            os.path.join(Config.ZIP_DESTINATION, "enterprise.csv"),
            'r',
            encoding="utf-8"
        ) as file:
            cursor_ent.copy_expert(
                copy_stmt.format(table_dct.pop("enterprise").__table__.name),
                file
            )
            cursor_ent.close()
            conn.commit()

        # Step 2: Establishments
        update_cbe_logger.info("Populating table 'establishments'.")
        cursor_est = conn.cursor()

        with open(
            os.path.join(Config.ZIP_DESTINATION, "establishment.csv"),
            "r",
            encoding="utf-8"
        ) as file:
            cursor_est.copy_expert(
                copy_stmt.format(table_dct.pop("establishment").__table__.name),
                file,
            )
            cursor_est.close()
            conn.commit()

        # Step 3: Branches
        update_cbe_logger.info("Populating table 'branches'.")
        cursor_bra = conn.cursor()

        with open(
            os.path.join(Config.ZIP_DESTINATION, "branch.csv"),
            "r",
            encoding="utf-8"
        ) as file:
            cursor_bra.copy_expert(
                copy_stmt.format(table_dct.pop("branch").__table__.name),
                file,
            )
            cursor_bra.close()
            conn.commit()

        # Step 4: remainder of files
        for key, table_info in table_dct.items():
            update_cbe_logger.info(f"Populating table '{key}'.")
            csv_file = os.path.join(Config.ZIP_DESTINATION, f"{key}.csv")
            cursor = conn.cursor()

            with open(csv_file, "r", encoding="utf-8") as file:
                cursor.copy_expert(
                    copy_stmt.format(table_info.__table__.name),
                    file
                )
                cursor.close()
                conn.commit()

        success = True
    except Exception as e:
        update_cbe_logger.error(f"While copying CSV into database => {e}")
    finally:
        conn.close()
        return success


def clean_up():
    """Clean up after update."""

    tmp_path = os.path.join(
        os.getcwd(),
        "db_automation/updater/cbe/tmp"
    )
    shutil.rmtree(tmp_path)
    os.makedirs(tmp_path)
    update_cbe_logger.info("New temporary folder created.")
