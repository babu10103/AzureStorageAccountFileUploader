import json
import requests
import variables
import logging
import sys
from datetime import timedelta, datetime


logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', stream=sys.stderr, level=logging.DEBUG)
logging.getLogger("requests").setLevel(logging.DEBUG)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def log_http_response(response):
    if response.status_code == 200:
        logging.info("Request was successful.")
    elif response.status_code == 500:
        logging.error(f"Internal Server Error.\n{response.text}")
    else:
        logging.error(f"{response.text}")

def _to_utc_datetime(value):
    return value.strftime('%Y-%m-%dT%H:%M:%SZ')

class AzureRestApiClient:
    def __init__(self, *args, **kwargs):
        self.subscription_id = variables.azure_subscription_id
        self.api_version = kwargs['api_version'] if 'api_version' in kwargs else ""
        self.tenant_id = variables.azure_tenant_id
        self.client_id = variables.azure_client_id
        self.client_secret = variables.azure_client_secret
        self.resource_group = kwargs['resource_group'] if 'resource_group' in kwargs else variables.resource_group
        self.token = self.get_access_token()

    def get_access_token(self):
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/token"

        payload = {
            'grant_type': 'client_credentials',
            'client_id': f'{self.client_id}',
            'client_secret': f'{self.client_secret}',
            'resource': 'https://management.azure.com'
        }

        response = requests.request("POST", url, data=payload)
        log_http_response(response)
        return response.json().get('access_token') if response else None

    def make_request(self, method, endpoint, headers=None, params=None, data=None):
        base_url = f"https://management.azure.com/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}"
        url = f"{base_url}/{endpoint}?api-version={self.api_version}"
        headers = headers or {}
        headers["Authorization"] = f"Bearer {self.token}"

        data_json = json.dumps(data) if data else data
        response = requests.request(method, url, headers=headers, params=params, data=data_json)
        log_http_response(response)
        return response

    def create_snapshot(self, snapshot_name, disk_name, resource_group):
        self.api_version = "2021-12-01"
        self.resource_group = resource_group
        body = {
            "location": "Central India",
            "properties": {
                "creationData": {
                    "createOption": "Copy",
                    "sourceResourceId": f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}/providers/Microsoft.Compute/disks/{disk_name}"
                }
            }
        }
        ep = f"providers/Microsoft.Compute/snapshots/{snapshot_name}"

        headers = {
            "Content-Type": "application/json"
        }

        response = self.make_request("PUT", endpoint=ep, data=body, headers=headers)
        return response

    def create_disk_from_snapshot(self, snapshot_name, disk_name, resource_group):
        self.api_version = "2021-12-01"
        self.resource_group = resource_group

        body = {
            "location": "Central India",
            "properties": {
                "creationData": {
                    "createOption": "Copy",
                    "sourceResourceId": f"subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}/providers/Microsoft.Compute/snapshots/{snapshot_name}"
                }
            }
        }
        ep = f"providers/Microsoft.Compute/disks/{disk_name}"

        headers = {
            "Content-Type": "application/json"
        }

        response = self.make_request("PUT", endpoint=ep, data=body, headers=headers)
        return response

    def account_sas_token(self, storage_account_name, expire_time):
        self.api_version = "2023-01-01"
        """
            json_object_set(root, "signedExpiry", json_string(formattedExpiration));
            json_object_set(root, "signedPermission", json_string("rwdl"));
            json_object_set(root, "signedResourceTypes", json_string("sco"));
            json_object_set(root, "signedServices", json_string("bf"));
        """
        expiry = datetime.utcnow() + timedelta(hours=expire_time)

        request_body = {
          "signedExpiry": "2023-11-11T17:35:12Z",
          "signedPermission": "rwdl",
          "signedResourceTypes": "sco",
          "signedServices": "bf"
        }
        headers = {
            "Content-Type": "application/json"
        }
        ep = f"providers/Microsoft.Storage/storageAccounts/{storage_account_name}/ListAccountSas"

        response = self.make_request("POST", endpoint=ep, data=request_body, headers=headers)
        return response
