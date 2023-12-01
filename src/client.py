import backoff
import json
import logging
from requests.exceptions import HTTPError
from requests.auth import HTTPBasicAuth

from keboola.http_client import HttpClient


class QuickbooksClientException(Exception):
    pass


class QuickbooksClient(HttpClient):

    def __init__(self, company_id: str, refresh_token: str, oauth, sandbox: bool, fail_on_error: bool = False):
        if not sandbox:
            base_url = f"https://quickbooks.api.intuit.com/v3/company/{company_id}"
        else:
            base_url = f"https://sandbox-quickbooks.api.intuit.com/v3/company/{company_id}"
        logging.debug(f"Using BaseUrl: {base_url}")

        super().__init__(base_url, max_retries=5)

        self.refresh_token = refresh_token
        self.access_token = None
        self.access_token_refreshed = False
        self.app_key = oauth.appKey
        self.app_secret = oauth.appSecret
        self.fail_on_error = fail_on_error

    def write_journal(self, entry: dict) -> dict:
        error = self._post("journalentry", data=entry)
        return error

    def write_invoice(self, entry: dict) -> dict:
        error = self._post("invoice", data=entry)
        return error

    @backoff.on_exception(backoff.expo, HTTPError, max_tries=3)
    def refresh_access_token(self):
        """
        Get a new access token with refresh token.
        Also saves the new token in statefile.
        """
        logging.info("Refreshing Access Token")

        url = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
        param = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token
        }

        r = self.post_raw(url, auth=HTTPBasicAuth(self.app_key, self.app_secret), data=param, is_absolute_path=True)
        r.raise_for_status()

        results = r.json()

        if "error" in results:
            raise QuickbooksClientException(f"Failed to refresh access token, please re-authorize credentials:"
                                            f" {r.text}")

        self.access_token = results["access_token"]
        self.refresh_token = results["refresh_token"]
        self.access_token_refreshed = True

    def _post(self, endpoint, data: dict):
        headers = {
            "Authorization": "Bearer " + self.access_token,
            "Content-type": "application/json"
        }
        params = {
            "minorversion": 69
        }

        try:
            json_data = json.dumps(data)
            logging.debug(f"Processing request: {endpoint}, {json_data}")
            r = self.post_raw(endpoint, data=json_data, headers=headers, params=params)
            r.raise_for_status()
            return False
        except HTTPError as e:
            if self.fail_on_error:
                return r.json()
            else:
                raise QuickbooksClientException(e)
