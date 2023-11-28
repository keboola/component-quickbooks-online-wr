import backoff
import logging
from typing import Tuple
from requests.exceptions import HTTPError
from requests.auth import HTTPBasicAuth

from keboola.http_client import HttpClient


class QuickbooksClientException(Exception):
    pass


class QuickbooksClient(HttpClient):

    def __init__(self, company_id: str, refresh_token: str, access_token: str, oauth, sandbox: bool):
        if not sandbox:
            base_url = f"https://quickbooks.api.intuit.com/v3/company/{company_id}"
        else:
            base_url = f"https://sandbox-quickbooks.api.intuit.com/v3/company/{company_id}"
        logging.info(f"Using BaseUrl: {base_url}")

        super().__init__(base_url)

        self.access_token = access_token
        self.refresh_token = refresh_token
        self.app_key = oauth.appKey
        self.app_secret = oauth.appSecret
        self.access_token_refreshed = False

    def write_journal(self, entry: dict):
        self._post("journalentry", data=entry)

    def write_invoice(self, entry: dict):
        self._post("invoice", data=entry)

    def get_new_refresh_token(self) -> Tuple[str, str]:
        try:
            self.refresh_access_token()
        except Exception as e:
            raise QuickbooksClientException(e) from e

        self.access_token_refreshed = False  # this is here in case the token would change during the component run
        return self.refresh_token, self.access_token

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

    def _post(self, endpoint, data):
        try:
            r = self.post_raw(endpoint, data=data)
            r.raise_for_status()
        except HTTPError as e:
            raise QuickbooksClientException(e)