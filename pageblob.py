import os
import requests
import datetime
import argparse
from AzureRestAPIClient import AzureRestApiClient

parser = argparse.ArgumentParser("A program to create a vulnerability status report")
parser.add_argument("--file_name", default="vuln_status_report")
parser.add_argument('-f', '--format', default='CSV', choices=["CSV", "JSON"], help="Report format")
parser.add_argument('-t', '--tries', default=4, type=int,
                    help="How many times to retry downloading the report, i.e. wait for the report to be generated")
parser.add_argument('-s', '--sleep_time', default=5, type=int,
                    help="The amount of time to sleep in-between (re-)tries to download the report")

args = parser.parse_args()

chunk_size_bytes = 1 * 1024 * 1024


def main():
    storage_account_name = "<your storage account name>"
    container_name = "<container name>"
    blob_name = "<blob name>"

    az_client = AzureRestApiClient()
    # SAS token to be generated from the portal or using SharedKey Generation - Account level SAS
    sas_token = az_client.get_access_token(storage_account_name, 8)

    source_url = 'C:\\Users\\Banda_Babu\\Downloads\\mretestvm_ss_new.vhdx'
    try:
        print("Start time", datetime.datetime.now())
        blob_url = f"https://{storage_account_name}.blob.core.windows.net/{container_name}/{blob_name}"

        create_page_blob(blob_url, sas_token, get_file_size(source_url))

        upload_large_file_as_pages(blob_url, sas_token, source_url)

        print("End time", datetime.datetime.now())

    except Exception as e:
        print("An error occurred:", str(e))


"""
First step is to create an empty page blob with the file size
"""


def create_page_blob(dest_url, sas_token, file_size):
    put_blob_url = f"{dest_url}?{sas_token}"
    headers = {
        "x-ms-version": "2020-04-08",
        "x-ms-date": datetime.datetime.now().strftime("%a, %d %b %Y %H:%M:%S GMT"),
        "x-ms-blob-type": "PageBlob",
        "x-ms-blob-content-length": str(file_size),
        "x-ms-blob-sequence-number": "0"
    }
    response = requests.put(put_blob_url, headers=headers)

    if response.status_code == 201:
        print("Status Code", response.status_code, "for the initial page blob")
    else:
        print("Failed to create Pageblob. Status code:", response.status_code)
        print("Response:", response.text)


def upload_large_file_as_pages(dest_url, sas_token, source_url):
    with open(source_url, 'rb') as source_stream:
        p_offset = 0
        while True:
            page_blob_data = source_stream.read(chunk_size_bytes)
            if not page_blob_data:
                break
            if chunk_size_bytes != len(page_blob_data):
                until = p_offset + len(page_blob_data) - 1
            else:
                until = p_offset + chunk_size_bytes - 1
            page_sequence = f"bytes={p_offset}-{until}"
            print(page_sequence)
            p_offset = until + 1
            create_put_page_request(dest_url, sas_token, page_sequence, page_blob_data)


def create_put_page_request(dest_url, sas_token, page_sequence, page_data):
    put_block_url = f"{dest_url}?comp=page&{sas_token}"
    headers = {
        "x-ms-version": "2020-04-08",
        "x-ms-page-write": "update",
        "x-ms-date": datetime.datetime.now().strftime("%a, %d %b %Y %H:%M:%S GMT"),
        "Range": page_sequence
    }
    response = requests.put(put_block_url, headers=headers, data=page_data)

    if response.status_code == 201:
        print("Status Code", response.status_code, "for the chunk")
    else:
        print("Failed to upload block. Status code:", response.status_code)
        print("Response:", response.text)


def get_file_size(source_url):
    return os.path.getsize(source_url)


if __name__ == "__main__":
    main()
