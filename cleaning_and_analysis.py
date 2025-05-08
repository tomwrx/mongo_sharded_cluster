import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pymongo import MongoClient
from tqdm import tqdm
import multiprocessing as mp
from pymongo.errors import BulkWriteError


def clean_chunk(df_chunk):
    df_chunk = pd.DataFrame(df_chunk)
    df_chunk = df_chunk[~df_chunk['nav_status'].isin(['Moored', 'At anchor', 'Reserved for future use'])]
    df_chunk = df_chunk.dropna(subset=['mmsi', 'lat', 'lon', 'timestamp', 'sog', 'cog']).copy()
    df_chunk = df_chunk[(df_chunk['lat'] >= -90) & (df_chunk['lat'] <= 90) & (df_chunk['lon'] >= -180) & (df_chunk['lon'] <= 180)]
    df_chunk = df_chunk[df_chunk['sog'] > 0]
    df_chunk['lat'] = pd.to_numeric(df_chunk['lat'], errors='coerce')
    df_chunk['lon'] = pd.to_numeric(df_chunk['lon'], errors='coerce')
    df_chunk['sog'] = pd.to_numeric(df_chunk['sog'], errors='coerce')
    df_chunk['cog'] = pd.to_numeric(df_chunk['cog'], errors='coerce')
    df_chunk['timestamp'] = pd.to_datetime(df_chunk['timestamp'], errors='coerce')
    df_chunk = df_chunk.dropna(subset=['lat', 'lon', 'sog', 'cog', 'timestamp'])
    return df_chunk


def load_and_clean_data_parallel(collection, batch_size=100000, num_processes=None):
    if num_processes is None:
        num_processes = max(1, mp.cpu_count() - 1)

    total_docs = collection.estimated_document_count()
    print(f"Processing {total_docs} documents in chunks of {batch_size} using {num_processes} processes...")

    all_cleaned_data = []
    pool = mp.Pool(num_processes)

    for skip in tqdm(range(0, total_docs, batch_size), desc="Cleaning chunks"):
        cursor = collection.find().skip(skip).limit(batch_size)
        df_chunk = pd.DataFrame(list(cursor))
        if df_chunk.empty:
            continue
        all_cleaned_data.append(df_chunk)

    cleaned_chunks = pool.map(clean_chunk, all_cleaned_data)
    pool.close()
    pool.join()

    if cleaned_chunks:
        return pd.concat(cleaned_chunks, ignore_index=True)
    else:
        return pd.DataFrame()


def compute_group_delta_t(group):
    group = group.sort_values('timestamp')
    delta_t = group['timestamp'].diff().dt.total_seconds() * 1000
    return delta_t.dropna().tolist()


def compute_delta_t_parallel(groups, num_processes=None):
    if num_processes is None:
        num_processes = max(1, mp.cpu_count() - 1)

    with mp.Pool(num_processes) as pool:
        results = pool.map(compute_group_delta_t, groups)

    all_delta_t = [dt for sublist in results for dt in sublist]
    return all_delta_t


def filter_vessels(df, min_points=100):
    grouped = df.groupby('mmsi')
    filtered_groups = [group for mmsi, group in grouped if len(group) >= min_points]
    print(f"Vessels with >= {min_points} points: {len(filtered_groups)}")
    return filtered_groups


def plot_histogram(delta_t_list, output_path="delta_t_histogram.png"):
    if not delta_t_list:
        print("No delta t values computed.")
        return
    delta_t_array = np.array(delta_t_list)
    plt.figure(figsize=(10, 6))
    plt.hist(delta_t_array, bins=50, edgecolor='black')
    plt.title('Histogram of Delta t (milliseconds)')
    plt.xlabel('Delta t (ms)')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path)
    print(f"Histogram saved as {output_path}")
    plt.close()


def main():
    client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=20000)
    db = client["ais_tracking"]
    raw_collection = db["positions"]
    cleaned_collection = db["positions_cleaned"]
    cleaned_collection.drop()

    df_all = load_and_clean_data_parallel(raw_collection)
    if df_all.empty:
        print("No cleaned data found.")
        client.close()
        return

    filtered_groups = filter_vessels(df_all, min_points=100)
    if not filtered_groups:
        print("No vessels meet the minimum data point requirement.")
        client.close()
        return

    records = pd.concat(filtered_groups).to_dict(orient='records')
    for record in records:
        record.pop('_id', None)  # Remove existing _id to avoid duplicate key errors

    try:
        cleaned_collection.insert_many(records, ordered=False)
        print("Inserted cleaned and filtered data into positions_cleaned collection.")
    except BulkWriteError as bwe:
        print(f"Bulk write error: {bwe.details}")

    cleaned_collection.create_index([("mmsi", 1), ("timestamp", 1)])
    print("Created index on (mmsi, timestamp) for positions_cleaned collection.")

    all_delta_t = compute_delta_t_parallel(filtered_groups)
    plot_histogram(all_delta_t, output_path="delta_t_histogram.png")

    client.close()

if __name__ == "__main__":
    main()
