import pandas as pd
import numpy as np
import os
import glob
import json

DATASET_DIR = "/opt/airflow/dataset"
OUTPUT_DIR = "/opt/airflow/src/output"

FINAL_COLUMNS = [
    "referral_details_id",
    "referral_id",
    "referral_source",
    "referral_source_category",
    "referral_at",
    "referrer_id",
    "referrer_name",
    "referrer_phone_number",
    "referrer_homeclub",
    "referee_id",
    "referee_name",
    "referee_phone",
    "referral_status",
    "num_reward_days",
    "transaction_id",
    "transaction_status",
    "transaction_at",
    "transaction_location",
    "transaction_type",
    "updated_at",
    "referral_rewards_created_at",
    "validity",
]


# =========================
# LOAD
# =========================
def load_data():
    try:
        lead_df = pd.read_csv(f"{DATASET_DIR}/lead_log.csv")
        paid_transcation = pd.read_csv(f"{DATASET_DIR}/paid_transactions.csv")
        referral_rewards = pd.read_csv(f"{DATASET_DIR}/referral_rewards.csv")
        user_logs = pd.read_csv(f"{DATASET_DIR}/user_logs.csv")
        user_referral_logs = pd.read_csv(f"{DATASET_DIR}/user_referral_logs.csv")
        user_referral_statuses = pd.read_csv(f"{DATASET_DIR}/user_referral_statuses.csv")
        user_referrals = pd.read_csv(f"{DATASET_DIR}/user_referrals.csv")

        print("All CSV files loaded successfully!")

        return (
            lead_df,
            paid_transcation,
            referral_rewards,
            user_logs,
            user_referral_logs,
            user_referral_statuses,
            user_referrals,
        )

    except FileNotFoundError as e:
        print(f"File not found: {e}")
        raise

    except pd.errors.EmptyDataError as e:
        print(f"One of the CSV files is empty: {e}")
        raise

    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file: {e}")
        raise

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise


# =========================
# RENAME COLUMNS (SOURCE TABLES)
# =========================
def rename_source_columns(
    lead_df,
    paid_transcation,
    referral_rewards,
    user_logs,
    user_referral_logs,
    user_referral_statuses,
):
    lead_df = lead_df.rename(columns={
        "id": "lead_logs_id",
        "created_at": "lead_logs_created_at"
    })
    user_referral_logs = user_referral_logs.rename(columns={
        "id": "user_referral_logs_id",
        "created_at": "user_referral_logs_created_at"
    })
    referral_rewards = referral_rewards.rename(columns={
        "id": "reward_id",
        "created_at": "referral_rewards_created_at"
    })
    user_referral_statuses = user_referral_statuses.rename(columns={
        "id": "referral_status_id",
        "created_at": "user_referral_status_created_at"
    })
    user_logs = user_logs.rename(columns={
        "id": "user_logs_id"
    })
    paid_transcation = paid_transcation.rename(columns={
        "transaction_id": "paid_transaction_id"
    })

    return (
        lead_df,
        paid_transcation,
        referral_rewards,
        user_logs,
        user_referral_logs,
        user_referral_statuses,
    )


# =========================
# CLEAN
# =========================
def clean_data(
    lead_df,
    paid_transcation,
    referral_rewards,
    user_logs,
    user_referral_logs,
    user_referral_statuses,
    user_referrals,
):
    try:
        # =========================
        # LEAD LOGS
        # =========================
        lead_df["lead_logs_created_at"] = pd.to_datetime(
            lead_df["lead_logs_created_at"], utc=True
        )

        lead_df = lead_df.dropna(subset=["lead_logs_id"])

        lead_df = lead_df.drop_duplicates(
            subset=["lead_logs_id"], keep="first"
        ).reset_index(drop=True)

        for col in ["source_category", "timezone_location", "current_status"]:
            lead_df[col] = lead_df[col].str.title().str.strip()

        # =========================
        # PAID TRANSACTIONS
        # =========================
        paid_transcation["transaction_at"] = pd.to_datetime(
            paid_transcation["transaction_at"], utc=True
        )

        paid_transcation = paid_transcation.dropna(subset=["paid_transaction_id"])

        paid_transcation = paid_transcation.drop_duplicates(
            subset=["paid_transaction_id"], keep="first"
        ).reset_index(drop=True)

        for col in [
            "timezone_transaction",
            "transaction_status",
            "transaction_type"
        ]:
            paid_transcation[col] = (
                paid_transcation[col].str.title().str.strip()
            )

        # =========================
        # REFERRAL REWARDS
        # =========================
        referral_rewards["referral_rewards_created_at"] = pd.to_datetime(
            referral_rewards["referral_rewards_created_at"], utc=True
        )

        referral_rewards["reward_value"] = (
            referral_rewards["reward_value"]
            .str.replace(" days", "", regex=False)
            .astype(int)
        )

        referral_rewards = referral_rewards.dropna(
            subset=["reward_id"]
        )

        referral_rewards = referral_rewards.drop_duplicates(
            subset=["reward_id"], keep="first"
        ).reset_index(drop=True)

        # =========================
        # USER LOGS
        # =========================
        user_logs["membership_expired_date"] = pd.to_datetime(
            user_logs["membership_expired_date"], utc=True
        )

        user_logs = user_logs.dropna(subset=["user_id"])

        user_logs = user_logs.drop_duplicates(
            subset=["user_id"], keep="first"
        ).reset_index(drop=True)

        user_logs["timezone_homeclub"] = (
            user_logs["timezone_homeclub"].str.title().str.strip()
        )

        user_logs["phone_number"] = (
            user_logs["phone_number"].fillna("Unknown")
        )

        # =========================
        # USER REFERRAL LOGS
        # =========================
        user_referral_logs["user_referral_logs_created_at"] = pd.to_datetime(
            user_referral_logs["user_referral_logs_created_at"], utc=True
        )

        user_referral_logs = user_referral_logs.dropna(
            subset=["user_referral_id"]
        )

        user_referral_logs = user_referral_logs.drop_duplicates(
            subset=["user_referral_id"], keep="first"
        ).reset_index(drop=True)

        user_referral_logs["source_transaction_id"] = (
            user_referral_logs["source_transaction_id"].fillna("Unknown")
        )

        # =========================
        # USER REFERRAL STATUSES
        # =========================
        user_referral_statuses["user_referral_status_created_at"] = pd.to_datetime(
            user_referral_statuses["user_referral_status_created_at"], utc=True
        )

        user_referral_statuses = user_referral_statuses.dropna(
            subset=["referral_status_id"]
        )

        user_referral_statuses = user_referral_statuses.drop_duplicates(
            subset=["referral_status_id"], keep="first"
        ).reset_index(drop=True)

        user_referral_statuses["description"] = (
            user_referral_statuses["description"].str.title().str.strip()
        )

        # =========================
        # USER REFERRALS
        # =========================
        user_referrals["referral_at"] = pd.to_datetime(
            user_referrals["referral_at"], utc=True
        )

        user_referrals["updated_at"] = pd.to_datetime(
            user_referrals["updated_at"], utc=True
        )

        user_referrals = user_referrals.dropna(
            subset=[
                "referral_id",
                "referee_id",
                "transaction_id",
                "referral_reward_id",
                "user_referral_status_id",
                "referrer_id"
            ]
        )

        user_referrals["referee_name"] = (
            user_referrals["referee_name"].fillna("Unknown")
        )

        print("Data cleaning completed successfully!")

        return (
            lead_df,
            paid_transcation,
            referral_rewards,
            user_logs,
            user_referral_logs,
            user_referral_statuses,
            user_referrals,
        )

    except KeyError as e:
        print(f"Missing column: {e}")
        raise

    except ValueError as e:
        print(f"Data conversion error: {e}")
        raise

    except TypeError as e:
        print(f"Unexpected data type: {e}")
        raise

    except Exception as e:
        print(f"An unexpected error occurred during data cleaning: {e}")
        raise


# =========================
# MERGE
# =========================
def merge_data(
    lead_df,
    paid_transcation,
    referral_rewards,
    user_logs,
    user_referral_logs,
    user_referral_statuses,
    user_referrals,
):
    try:
        result = (
            user_referrals
            .merge(
                lead_df,
                left_on="referee_id",
                right_on="lead_id",
                how="left"
            )
            .merge(
                user_referral_logs,
                left_on="referral_id",
                right_on="user_referral_id",
                how="left"
            )
            .merge(
                paid_transcation,
                left_on="transaction_id",
                right_on="paid_transaction_id",
                how="left"
            )
            .merge(
                user_logs,
                left_on="referrer_id",
                right_on="user_id",
                how="left"
            )
            .merge(
                user_referral_statuses,
                left_on="user_referral_status_id",
                right_on="referral_status_id",
                how="left"
            )
            .merge(
                referral_rewards,
                left_on="referral_reward_id",
                right_on="reward_id",
                how="left"
            )
        )

        print("All tables merged successfully!")
        print(f"Final dataset contains {len(result)} rows and {len(result.columns)} columns.")

        return result

    except KeyError as e:
        print(f"Missing merge key or column: {e}")
        raise

    except pd.errors.MergeError as e:
        print(f"Merge error: {e}")
        raise

    except ValueError as e:
        print(f"Value error during merging: {e}")
        raise

    except Exception as e:
        print(f"An unexpected error occurred during merging: {e}")
        raise


# =========================
# REFERRAL SOURCE CATEGORY
# =========================
def add_referral_source_category(result):
    try:
        result["referral_source_category"] = np.select(
            [
                result["referral_source"] == "User Sign Up",
                result["referral_source"] == "Draft Transaction"
            ],
            [
                "Online",
                "Offline"
            ],
            default=result["source_category"]
        )

        print("Referral source category created successfully!")

        return result

    except KeyError as e:
        print(f"Missing column: {e}")
        raise

    except ValueError as e:
        print(f"Value error while creating referral source category: {e}")
        raise

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise


# =========================
# BUSINESS LOGIC VALIDATION
# =========================
def validate_business_logic(result):
    try:
        # =========================
        # REWARD CONDITIONS
        # =========================
        has_reward = result["reward_value"] > 0

        no_reward = (
            result["reward_value"].isna()
            | (result["reward_value"] == 0)
        )

        # Normalize transaction date to midnight for membership expiry comparison
        ref_date = result["transaction_at"].dt.normalize()

        after = result["transaction_at"] > result["referral_at"]
        before = result["transaction_at"] < result["referral_at"]

        # =========================
        # VALID SUCCESSFUL REFERRALS
        # =========================
        valid_successful = (
            has_reward
            & (result["description"] == "Berhasil")
            & result["transaction_id"].notna()
            & (result["transaction_status"] == "Paid")
            & (result["transaction_type"] == "New")
            & after
            & (
                result["transaction_at"].dt.to_period("M")
                == result["referral_at"].dt.to_period("M")
            )
            & (result["membership_expired_date"] >= ref_date)
            # & ~result["is_deleted"]
            # & result["is_reward_granted"]
        )

        # =========================
        # VALID UNSUCCESSFUL REFERRALS
        # =========================
        valid_unsuccessful = (
            result["description"].isin(["Menunggu", "Tidak Berhasil"])
            & no_reward
        )

        # =========================
        # INVALID REFERRALS
        # =========================
        invalid = (
            (has_reward & (result["description"] != "Berhasil"))
            | (has_reward & result["transaction_id"].isna())
            | (
                no_reward
                & result["transaction_id"].notna()
                & (result["transaction_status"] == "Paid")
                & after
            )
            | (
                (result["description"] == "Berhasil")
                & no_reward
            )
            | before
        )

        # =========================
        # FINAL BUSINESS LOGIC VALIDATION
        # =========================
        result["is_business_logic_valid"] = (
            valid_successful | valid_unsuccessful
        ) & ~invalid

        result["validity"] = result["is_business_logic_valid"].map({
            True: True,
            False: False
        })

        print("Business logic validation completed successfully!")

        print(
            f"Valid records: {result['is_business_logic_valid'].sum()}"
        )
        print(
            f"Invalid records: {(~result['is_business_logic_valid']).sum()}"
        )

        return result

    except KeyError as e:
        print(f"Missing column: {e}")
        raise

    except AttributeError as e:
        print(f"Date column error: {e}. Check that date columns are datetime.")
        raise

    except TypeError as e:
        print(f"Data type error: {e}")
        raise

    except Exception as e:
        print(f"Business logic validation failed: {e}")
        raise


# =========================
# RENAME + SELECT FINAL COLUMNS
# =========================
def select_final_columns(result):
    try:
        rename_map = {
            "user_referral_status_id": "referral_details_id",
            "name": "referrer_name",
            "phone_number": "referrer_phone_number",
            "homeclub": "referrer_homeclub",
            "description": "referral_status",
            "reward_value": "num_reward_days",
        }

        result = result.rename(columns=rename_map)

        # Check for missing columns
        missing_columns = [
            col for col in FINAL_COLUMNS
            if col not in result.columns
        ]

        if missing_columns:
            raise KeyError(f"Missing columns: {missing_columns}")

        result = result[FINAL_COLUMNS]

        print("Column renaming and selection completed successfully!")

        return result

    except KeyError as e:
        print(f"Column error: {e}")
        raise

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise


# =========================
# INCREMENTAL LOAD (WATERMARK)
# =========================
def append_new_records(result):
    watermark_path = f"{OUTPUT_DIR}/watermark.json"
    report_path = f"{OUTPUT_DIR}/final_report.csv"

    try:
        # =========================
        # READ WATERMARK
        # =========================
        if os.path.exists(watermark_path):
            with open(watermark_path, "r") as file:
                watermark = json.load(file)

            last_updated_at = pd.to_datetime(
                watermark["last_updated_at"], utc=True
            )
        else:
            last_updated_at = pd.Timestamp.min.tz_localize("UTC")

        # =========================
        # CONVERT UPDATED_AT
        # =========================
        result["updated_at"] = pd.to_datetime(
            result["updated_at"], utc=True
        )

        # =========================
        # FILTER NEW RECORDS
        # =========================
        new_records = result[
            result["updated_at"] > last_updated_at
        ]

        # =========================
        # SELECT FINAL COLUMNS
        # =========================
        report = new_records[FINAL_COLUMNS]

        # =========================
        # APPEND TO CSV
        # =========================
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        if not report.empty:
            report.to_csv(
                report_path,
                mode="a",
                index=False,
                header=not os.path.exists(report_path)
            )

            # Update watermark only after successful append
            watermark = {
                "last_updated_at": new_records["updated_at"].max().isoformat()
            }

            with open(watermark_path, "w") as file:
                json.dump(watermark, file, indent=4)

            print(f"Successfully appended {len(report)} new records.")

        else:
            print("No new records found. Nothing to append.")

        return result

    except FileNotFoundError as e:
        print(f"File not found: {e}")
        raise

    except KeyError as e:
        print(f"Missing column or watermark key: {e}")
        raise

    except ValueError as e:
        print(f"Data conversion or JSON error: {e}")
        raise

    except PermissionError as e:
        print(f"Permission denied: {e}")
        raise

    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise


# =========================
# PROFILING
# =========================
def profile(name, df):
    return pd.DataFrame({
        "table_name": name,
        "column_name": df.columns,
        "data_type": df.dtypes.astype(str).values,
        "null_count": df.isna().sum().values,
        "percentage_populated": (100 * df.notna().mean()).round(2).values,
        "distinct_value_count": df.nunique().values,
        "minimum_value": [
            df[c].dropna().min() if df[c].notna().any() else ""
            for c in df.columns
        ],
        "maximum_value": [
            df[c].dropna().max() if df[c].notna().any() else ""
            for c in df.columns
        ],
        "max_actual_length": [
            df[c].dropna().astype(str).str.len().max()
            if df[c].notna().any() else 0
            for c in df.columns
        ],
    })


def generate_profiling_report(
    lead_df,
    paid_transcation,
    referral_rewards,
    user_logs,
    user_referral_logs,
    user_referral_statuses,
    user_referrals,
):
    try:
        tables = {
            "lead_log": lead_df,
            "paid_transactions": paid_transcation,
            "referral_rewards": referral_rewards,
            "user_logs": user_logs,
            "user_referral_logs": user_referral_logs,
            "user_referral_statuses": user_referral_statuses,
            "user_referrals": user_referrals,
        }

        # Generate profiling report
        report = pd.concat(
            [profile(name, df) for name, df in tables.items()],
            ignore_index=True
        )

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Delete old profiling reports
        for old in glob.glob(f"{OUTPUT_DIR}/profiling_report*.csv"):
            os.remove(old)

        # Save new profiling report
        report.to_csv(f"{OUTPUT_DIR}/profiling_report.csv", index=False)

        print("Profiling completed successfully!")
        print("Report saved to profiling_report.csv")

        return report

    except KeyError as e:
        print(f"Missing expected column: {e}")
        raise

    except PermissionError as e:
        print(f"Permission denied: {e}")
        raise

    except Exception as e:
        print(f"Profiling failed: {e}")
        raise


# =========================
# MAIN
# =========================
def main():
    (
        lead_df,
        paid_transcation,
        referral_rewards,
        user_logs,
        user_referral_logs,
        user_referral_statuses,
        user_referrals,
    ) = load_data()

    (
        lead_df,
        paid_transcation,
        referral_rewards,
        user_logs,
        user_referral_logs,
        user_referral_statuses,
    ) = rename_source_columns(
        lead_df,
        paid_transcation,
        referral_rewards,
        user_logs,
        user_referral_logs,
        user_referral_statuses,
    )

    (
        lead_df,
        paid_transcation,
        referral_rewards,
        user_logs,
        user_referral_logs,
        user_referral_statuses,
        user_referrals,
    ) = clean_data(
        lead_df,
        paid_transcation,
        referral_rewards,
        user_logs,
        user_referral_logs,
        user_referral_statuses,
        user_referrals,
    )

    result = merge_data(
        lead_df,
        paid_transcation,
        referral_rewards,
        user_logs,
        user_referral_logs,
        user_referral_statuses,
        user_referrals,
    )

    result = add_referral_source_category(result)
    result = validate_business_logic(result)
    result = select_final_columns(result)
    append_new_records(result)

    generate_profiling_report(
        lead_df,
        paid_transcation,
        referral_rewards,
        user_logs,
        user_referral_logs,
        user_referral_statuses,
        user_referrals,
    )


if __name__ == "__main__":
    main()