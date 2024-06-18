import os
import requests
import datetime
from AzureRestAPIClient import AzureRestApiClient

# chunk size should be below 4 MB
chunk_size_bytes = 1 * 1024 * 1024


def upload_blob():
    storage_account = "<your storage account name>"
    container = "<container name>"
    blob_name = "<blob name>"
    source_path = '/path/to/your/vhd'

    client = AzureRestApiClient()
    sas_token = client.account_sas_token(storage_account, 8)

    try:
        blob_url = f"https://{storage_account}.blob.core.windows.net/{container}/{blob_name}"
        blob_size = get_file_size(source_path)

        if not create_page_blob(blob_url, sas_token, blob_size):
            raise Exception("Failed to create PageBlob.")
        upload_large_file_as_pages(blob_url, sas_token, source_path)

    except Exception as e:
        print(f"An error occurred: {e}")


def create_page_blob(dest_url: str, sas_token: str, file_size: int) -> bool:
    blob_url = f"{dest_url}?{sas_token}"
    headers = {
        "x-ms-version": "2020-04-08",
        "x-ms-date": datetime.datetime.now().strftime("%a, %d %b %Y %H:%M:%S GMT"),
        "x-ms-blob-type": "PageBlob",
        "x-ms-blob-content-length": str(file_size),
        "x-ms-blob-sequence-number": "0"
    }

    try:
        response = requests.put(blob_url, headers=headers)
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"Failed to create PageBlob. Error: {str(e)}")
        return False


def upload_large_file_as_pages(destination_url, sas_token, source_file_path):
    with open(source_file_path, 'rb') as source_file:
        offset = 0
        while True:
            chunk_data = source_file.read(chunk_size_bytes)
            if not chunk_data:
                break
            if chunk_size_bytes != len(chunk_data):
                end_offset = offset + len(chunk_data) - 1
            else:
                end_offset = offset + chunk_size_bytes - 1
            page_sequence = f"bytes={offset}-{end_offset}"
            offset = end_offset + 1
            create_put_page_request(destination_url, sas_token, page_sequence, chunk_data)


def create_put_page_request(destination_url, sas_token, page_sequence, page_data):
    headers = {
        "x-ms-version": "2020-04-08",
        "x-ms-page-write": "update",
        "x-ms-date": datetime.datetime.now().strftime("%a, %d %b %Y %H:%M:%S GMT"),
        "Range": page_sequence
    }
    response = requests.put(f"{destination_url}?comp=page&{sas_token}", headers=headers, data=page_data)
    if response.status_code == 201:
        return
    else:
        raise Exception(f"Failed to upload block. Status code: {response.status_code}. Response: {response.text}")


def get_file_size(source_url):
    return os.path.getsize(source_url)


if __name__ == "__main__":
    upload_blob()
