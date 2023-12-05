import backoff
import csv
import datetime
import json
import logging
import os
import requests

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from client import QuickbooksClient

# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_SANDBOX = 'sandbox'
KEY_ENDPOINTS = 'endpoints'
KEY_COMPANY_ID = 'company_id'
KEY_FAIL_ON_ERROR = 'fail_on_error'


REQUIRED_PARAMETERS = [KEY_COMPANY_ID, KEY_ENDPOINTS]

endpoint_table_mapping = {
    "invoices": "invoices.csv",
    "journals": "journals.csv"
}

URL_SUFFIXES = {"US": ".keboola.com",
                "EU": ".eu-central-1.keboola.com",
                "AZURE-EU": ".north-europe.azure.keboola.com",
                "CURRENT_STACK": os.environ.get('KBC_STACKID', 'connection.keboola.com').replace('connection', '')}


class Component(ComponentBase):
    ERRORS_TABLE_NAME = "errors"

    def __init__(self):
        super().__init__()
        self.client: QuickbooksClient
        self.refresh_token = None
        self.errors_table = None

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        oauth = self.configuration.oauth_credentials
        sandbox = self.configuration.parameters.get(KEY_SANDBOX, False)
        company_id = self.configuration.parameters.get(KEY_COMPANY_ID)
        endpoints = self.configuration.parameters.get(KEY_ENDPOINTS, [])
        fail_on_error = self.configuration.parameters.get(KEY_FAIL_ON_ERROR, False)

        self.refresh_token = self.get_refresh_token(oauth)

        if not endpoints:
            raise UserException("Endpoints parameter cannot be empty.")

        client = QuickbooksClient(company_id, self.refresh_token, oauth, sandbox, fail_on_error)
        self.process_oauth_tokens(client, sandbox)

        tables_in_path = self.tables_in_path
        for endpoint in endpoints:

            if endpoint not in endpoint_table_mapping.keys():
                raise UserException(f"Unsupported endpoint: {endpoint}")

            in_table_path = os.path.join(tables_in_path, endpoint_table_mapping.get(endpoint))

            if not os.path.exists(in_table_path):
                raise UserException(f"Input table for selected endpoint {endpoint} not found. Table's name should be "
                                    f"{endpoint_table_mapping.get(endpoint)}")

            with open(in_table_path, 'r') as f:
                reader = csv.DictReader(f)
                self.process_endpoint(client, reader, endpoint, fail_on_error)

        if self.errors_table:
            self.write_manifest(self.errors_table)

        self.write_state_file({
            "token":
                {"ts": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                 "#refresh_token": client.refresh_token}
        })

    def process_endpoint(self, client, reader: csv.DictReader, endpoint, fail_on_error: bool):

        logging.info(f"Processing endpoint: {endpoint}")

        if endpoint == "journals":
            function = client.write_journal
        elif endpoint == "invoices":
            function = client.write_invoice
        else:
            raise UserException(f"Unsupported endpoint: {endpoint}")

        if fail_on_error:
            for row in reader:
                data = json.loads(row['data'])
                client.write_journal(data)
        else:
            self.errors_table = self.create_out_table_definition(self.ERRORS_TABLE_NAME, primary_key=["id"],
                                                                 incremental=True)
            with open(self.errors_table.full_path, 'w') as f:
                writer = csv.DictWriter(f, fieldnames=["id", "endpoint", "error"])
                for row in reader:
                    try:
                        data = json.loads(row['data'])
                    except json.decoder.JSONDecodeError as e:
                        raise UserException(f"Cannot decode row {row['id']}: {e}")
                    error = function(data)
                    if error:
                        error_to_write = {
                            "id": row['id'],
                            "endpoint": endpoint,
                            "error": str(error)
                        }
                        writer.writerow(error_to_write)

    def process_oauth_tokens(self, client: QuickbooksClient, sandbox: bool) -> None:
        """Uses Quickbooks client to get new tokens and saves them using API if they have changed since the last run."""
        new_refresh_token = client.refresh_access_token()
        if self.refresh_token != new_refresh_token:
            if not sandbox:
                self.save_new_oauth_token(new_refresh_token)

            # We also save new tokens to class vars, so we can save them unencrypted if case statefile update fails
            # in update_config_state() method.
            self.refresh_token = new_refresh_token

    def get_refresh_token(self, oauth):

        try:
            refresh_token = oauth["data"]["refresh_token"]
        except TypeError:
            raise UserException("OAuth data is not available.")

        statefile = self.get_state_file()
        if statefile.get("token", {}).get("ts"):
            ts_oauth = datetime.datetime.strptime(oauth["created"], "%Y-%m-%dT%H:%M:%S.%fZ")
            ts_statefile = datetime.datetime.strptime(statefile["token"]["ts"], "%Y-%m-%dT%H:%M:%S.%fZ")

            if ts_statefile > ts_oauth:
                refresh_token = statefile["token"].get("#refresh_token")
                logging.debug("Loaded token from statefile.")
            else:
                logging.debug("Using token from oAuth.")
        else:
            logging.warning("No timestamp found in statefile. Using oAuth token.")

        return refresh_token

    def save_new_oauth_token(self, refresh_token: str) -> None:
        logging.debug("Saving new token to state using Keboola API.")

        encrypted_refresh_token = self.encrypt(refresh_token)

        new_state = {
            "component": {
                "token":
                    {"ts": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                     "#refresh_token": encrypted_refresh_token}
            }}
        self.update_config_state(region="CURRENT_STACK",
                                 component_id=self.environment_variables.component_id,
                                 configurationId=self.environment_variables.config_id,
                                 state=new_state,
                                 branch_id=self.environment_variables.branch_id)

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
    def encrypt(self, token: str) -> str:
        url = "https://encryption.keboola.com/encrypt"
        params = {
            "componentId": self.environment_variables.component_id,
            "projectId": self.environment_variables.project_id,
            "configId": self.environment_variables.config_id
        }
        headers = {"Content-Type": "text/plain"}

        response = requests.post(url,
                                 data=token,
                                 params=params,
                                 headers=headers)
        try:
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"Unable to encrypt token using Keboola encrypt API: {e}")
            self.write_state_file({
                "token":
                    {"ts": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                     "#refresh_token": self.refresh_token}
            })
            exit(0)
        else:
            return response.text

    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
    def update_config_state(self, region, component_id, configurationId, state, branch_id='default'):
        if not branch_id:
            branch_id = 'default'

        url = f'https://connection{URL_SUFFIXES[region]}/v2/storage/branch/{branch_id}' \
              f'/components/{component_id}/configs/' \
              f'{configurationId}/state'

        parameters = {'state': json.dumps(state)}
        headers = {'Content-Type': 'application/x-www-form-urlencoded', 'X-StorageApi-Token': self._get_storage_token()}
        response = requests.put(url,
                                data=parameters,
                                headers=headers)
        try:
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"Unable to update component state using Keboola Storage API: {e}")
            self.write_state_file({
                "token":
                    {"ts": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                     "#refresh_token": self.refresh_token}
            })
            exit(0)

    def _get_storage_token(self) -> str:
        token = self.configuration.parameters.get('#storage_token') or self.environment_variables.token
        if not token:
            raise UserException("Cannot retrieve storage token from env variables and/or config.")
        return token


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
