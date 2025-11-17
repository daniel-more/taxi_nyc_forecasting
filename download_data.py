import os
import requests
import urllib3

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
YEARS = [2023, 2024, 2025]
DATASETS = ["yellow_tripdata", "green_tripdata"]

for year in YEARS:
    OUTPUT_DIR = f"data/tlc_{year}"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_file(url, output_path):
    print(f"Downloading {os.path.basename(output_path)}...")
    try:
        response = requests.get(url, stream=True, verify=False)  # SSL bypass
        if response.status_code == 200:
            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"✅ Saved: {output_path}")
        else:
            print(f"❌ Failed: {url} (Status: {response.status_code})")
    except Exception as e:
        print(f"❌ Error downloading {url}: {e}")

for dataset in DATASETS:
    for year in YEARS:
        for month in range(1, 13):
            file_name = f"{dataset}_{year}-{month:02d}.parquet"
            url = BASE_URL + file_name
            output_path = os.path.join(OUTPUT_DIR, file_name)
            download_file(url, output_path)

print("✅ All downloads completed!")