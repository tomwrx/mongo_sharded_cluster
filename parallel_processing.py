import os
import pandas as pd
import multiprocessing as mp
import time
from tqdm import tqdm
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError


def preprocess_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    irrelevant_statuses = ["Moored", "At anchor", "Not under command"]
    chunk = chunk.dropna().drop_duplicates()
    chunk = chunk[~chunk["Navigational status"].isin(irrelevant_statuses)]
    chunk["Timestamp"] = pd.to_datetime(
        chunk["# Timestamp"], dayfirst=True, errors="coerce"
    )
    chunk.drop("# Timestamp", axis=1, inplace=True)
    return chunk


def load_and_preprocess_data_parallel(
    csv_path: str, num_processes: int = None
) -> pd.DataFrame:
    if num_processes is None:
        num_processes = mp.cpu_count()

    chunksize = 1000000  # 1 million rows per chunk
    results = []

    with mp.Pool(num_processes) as pool:
        for chunk_result in pool.imap(
            preprocess_chunk,
            pd.read_csv(
                csv_path,
                usecols=[
                    "# Timestamp",
                    "MMSI",
                    "Latitude",
                    "Longitude",
                    "SOG",
                    "COG",
                    "ROT",
                    "Heading",
                    "Navigational status",
                ],
                chunksize=chunksize,
            ),
        ):
            results.append(chunk_result)

    df = pd.concat(results, ignore_index=True)
    return df


def insert_batch(batch_df: pd.DataFrame):

    for attempt in range(3):
        try:
            client = MongoClient(
                "mongodb://localhost:27017", serverSelectionTimeoutMS=20000
            )
            db = client["ais_tracking"]
            collection = db["positions"]
            documents = batch_df.to_dict(orient="records")
            if documents:
                collection.insert_many(documents, ordered=False)
            client.close()
            break
        except ServerSelectionTimeoutError as e:
            print(f"Retry {attempt+1}/3: MongoDB connection failed → {e}")
            time.sleep(5)


def insert_in_parallel(df: pd.DataFrame, batch_size=1000, num_processes=None):
    if num_processes is None:
        num_processes = mp.cpu_count()

    # Rename for MongoDB compatibility
    df = df.rename(
        columns={
            "MMSI": "mmsi",
            "Latitude": "lat",
            "Longitude": "lon",
            "SOG": "sog",
            "COG": "cog",
            "ROT": "rot",
            "Heading": "heading",
            "Navigational status": "nav_status",
            "Timestamp": "timestamp",
        }
    )

    batches = [df[i : i + batch_size] for i in range(0, len(df), batch_size)]

    with mp.Pool(num_processes) as pool:
        for _ in tqdm(
            pool.imap_unordered(insert_batch, batches),
            total=len(batches),
            desc="Inserting batches",
        ):
            pass


def wait_for_mongo(retries=5, delay=5):

    for i in range(retries):
        try:
            client = MongoClient(
                "mongodb://localhost:27017", serverSelectionTimeoutMS=3000
            )
            client.admin.command("ping")
            print("MongoDB is ready")
            client.close()
            return
        except ServerSelectionTimeoutError:
            print(f"Waiting for MongoDB to be ready... (attempt {i+1}/{retries})")
            time.sleep(delay)
    print("MongoDB is still not available. Exiting.")
    exit(1)


if __name__ == "__main__":

    wait_for_mongo()
    data_folder = os.path.join(os.getcwd(), "data")
    csv_file = os.path.join(data_folder, os.listdir(data_folder)[0])

    print("Loading and preprocessing data...")
    df_cleaned = load_and_preprocess_data_parallel(csv_file)

    print("Inserting data into MongoDB in parallel...")
    insert_in_parallel(df_cleaned, batch_size=1000)

    print("Finished inserting!")
