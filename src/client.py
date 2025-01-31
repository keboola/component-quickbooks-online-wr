import backoff
import logging
from requests.exceptions import HTTPError
from requests.auth import HTTPBasicAuth
import requests.exceptions
from keboola.http_client import HttpClient
from xml.etree import ElementTree as ET


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

    def send(self, endpoint: str, entry: dict) -> dict:
        error = self._post(endpoint, data=entry)
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

    def _parse_xml_response(self, xml_text: str) -> dict:
        """Parse XML response from Quickbooks API.
        
        Args:
            xml_text: XML response text
            
        Returns:
            dict: Parsed error response in format {'Fault': {'Error': [...]}}
            
        Raises:
            QuickbooksClientException: If XML doesn't contain expected error format
        """
        root = ET.fromstring(xml_text)
        
        namespace = 'http://schema.intuit.com/finance/v3'
        errors = []
        
        for error in root.findall(f'.//{{{namespace}}}Error'):
            error_info = {
                'code': error.get('code', 'unknown'),
                'element': error.get('element', 'unknown'),
                'message': error.find(f'{{{namespace}}}Message').text,
                'detail': error.find(f'{{{namespace}}}Detail').text
            }
            errors.append(error_info)
        
        if not errors:
            raise QuickbooksClientException(f"Unexpected XML response format: {xml_text}")
        
        return {'Fault': {'Error': errors}}

    def _post(self, endpoint, data: dict):
        headers = {
            "Authorization": "Bearer " + self.access_token,
            "Content-type": "application/json"
        }

        try:
            logging.debug(f"Processing request: {endpoint}, {data}")
            r = self.post_raw(endpoint, json=data, headers=headers)
            logging.debug(f"Response: {r.text}")
            r.raise_for_status()
            return False
        except requests.exceptions.HTTPError as e:
            if r.status_code == 401:
                raise QuickbooksClientException(
                    "Unauthorized for Quickbooks API, please re-authorize credentials "
                    "and check your company_id."
                )
            
            # Handle error response
            try:
                if 'xml' in r.headers.get('Content-Type', '').lower():
                    return self._parse_xml_response(r.text)
                return r.json()
            except (ET.ParseError, requests.exceptions.JSONDecodeError, QuickbooksClientException) as parse_error:
                if self.fail_on_error:
                    raise QuickbooksClientException(
                        f"Failed to parse error response: {parse_error}, response: {r.text}"
                    )
                return {'Fault': {'Error': [{'message': r.text}]}}
