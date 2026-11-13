import pandas as pd

DATASET_PATH = "producer/data/transactions.csv"


def inspect_dataset():
    df = pd.read_csv(DATASET_PATH)

    total_records = len(df)
    unique_transactions = df["transaction_id"].nunique()
    duplicate_records = total_records - unique_transactions

    print("\n========== DATASET SUMMARY ==========")
    print(f"Total records          : {total_records}")
    print(f"Unique transactions    : {unique_transactions}")
    print(f"Duplicate records      : {duplicate_records}")

    print("\n========== COLUMNS ==========")
    for column in df.columns:
        print(f"- {column}")

    print("\n========== FIRST 3 RECORDS ==========")
    print(df.head(3).to_string(index=False))


if __name__ == "__main__":
    inspect_dataset()
