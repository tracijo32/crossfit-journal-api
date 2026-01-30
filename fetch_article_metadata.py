import requests
import json
import os
from google.cloud import storage


def fetch_article_metadata(per_page: int = 20, page: int = 1):
    params = {
        'sort': 'publishingDate',
        'per-page': per_page,
        'page': page
    }
    base_url = 'https://journal.crossfit.com/media-api/api/v1/media/journal'

    r = requests.get(base_url, params=params)
    assert r.status_code == 200, f'Error {r.status_code}: {r.reason}'

    return r.json()


def upload_to_gcs(project: str, bucket_name: str, blob_name: str, data: dict):
    """Upload JSON data to Google Cloud Storage bucket."""
    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    # Convert dict to JSON string
    json_data = json.dumps(data, indent=2, ensure_ascii=False)
    
    # Upload as text with JSON content type
    blob.upload_from_string(json_data, content_type='application/json')
    
    return blob


def blob_exists(project: str, bucket_name: str, blob_name: str) -> bool:
    """Check if a blob exists in the GCS bucket."""
    try:
        client = storage.Client(project=project)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.exists()
    except Exception as e:
        print(f"Error checking blob existence: {e}")
        return False


def main():
    # Google Cloud Storage project, bucket name
    project = 'crossfit-journal-rag-app'
    bucket_name = 'cf-journal'
    
    per_page = 20
    page = 1
    
    print(f"Starting to fetch articles (per_page={per_page})...")
    print(f"Uploading to GCS bucket: {bucket_name}")
    
    while True:
        # Check if file already exists in GCS
        blob_name = f"metadata/page={page}.json"
        
        if blob_exists(project, bucket_name, blob_name):
            print(f"Page {page} already exists in GCS. Skipping...")
            page += 1
            continue
        
        print(f"Fetching page {page}...")
        articles = fetch_article_metadata(per_page=per_page, page=page)
        
        # Check if we got an empty list
        if not articles or len(articles) == 0:
            print(f"No more articles found. Stopping at page {page}.")
            break
        
        # Upload to Google Cloud Storage
        try:
            upload_to_gcs(project, bucket_name, blob_name, articles)
            print(f"Uploaded {len(articles)} articles to {project} bucket gs://{bucket_name}/{blob_name}")
        except Exception as e:
            print(f"Error uploading to GCS: {e}")
            print("Continuing to next page...")
            page += 1
            continue
        
        page += 1
    
    print(f"\nFinished! All articles uploaded to GCS {project} bucket'{bucket_name}'.")


if __name__ == "__main__":
    main()
