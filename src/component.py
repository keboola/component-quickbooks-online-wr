import backoff
import csv
import datetime
import json
import logging
import os
import requests

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from client import QuickbooksClient, QuickbooksClientException
from mapping import expected_columns, create_entries

# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_SANDBOX = 'sandbox'
KEY_ENDPOINTS = 'endpoints'
KEY_ACTION = 'action'
KEY_COMPANY_ID = 'company_id'
KEY_FAIL_ON_ERROR = 'fail_on_error'

REQUIRED_PARAMETERS = [KEY_COMPANY_ID, KEY_ENDPOINTS, KEY_ACTION]

supported_endpoints = ["journalentry"]

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
        self.start_ts = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        oauth = self.configuration.oauth_credentials
        sandbox = self.configuration.parameters.get(KEY_SANDBOX, False)
        company_id = self.configuration.parameters.get(KEY_COMPANY_ID)
        endpoints = self.configuration.parameters.get(KEY_ENDPOINTS)
        action = self.configuration.parameters.get(KEY_ACTION)
        fail_on_error = self.configuration.parameters.get(KEY_FAIL_ON_ERROR, False)

        old_refresh_token = self.get_refresh_token(oauth)

        client = QuickbooksClient(company_id, old_refresh_token, oauth, sandbox, fail_on_error)
        self.refresh_and_save_quickbooks_token(client)

        tables_in_path = self.tables_in_path

        for endpoint in endpoints:
            if endpoint not in supported_endpoints:
                raise UserException(f"Unsupported endpoint: {endpoint}")

            table_name = f"{endpoint}.csv"
            in_table_path = os.path.join(tables_in_path, table_name)

            if not os.path.exists(in_table_path):
                raise UserException(f"Input table for selected endpoint {endpoint} not found. "
                                    f"Table's name should be {table_name}")

            try:
                self.process_endpoint(client, in_table_path, endpoint, action, fail_on_error)
            except QuickbooksClientException as e:
                raise UserException(f"Error processing endpoint {endpoint}: {e}")

        if self.errors_table:
            self.write_manifest(self.errors_table)

        self.write_state_file({
            "token":
                {"ts": self.start_ts,
                 "#refresh_token": client.refresh_token}
        })

    def process_endpoint(self, client, csv_path: str, endpoint: str, action: str, fail_on_error: bool):
        logging.info(f"Processing endpoint: {endpoint}")

        self.check_columns(endpoint, action, csv_path)
        batches = self.get_batches(csv_path)

        if fail_on_error:
            self.process_with_failure(client, csv_path, endpoint, action, batches)
        else:
            self.process_with_logging(client, csv_path, endpoint, action, batches)

    def process_with_failure(self, client, csv_path, endpoint, action, batches):
        for batch in batches:
            data = self.get_batch(csv_path, batch)
            entries = create_entries(endpoint, action, data)
            try:
                response = client.send(endpoint, entries)
            except QuickbooksClientException as e:
                raise UserException(f"Error processing endpoint {endpoint}, action {action}, with data {data}: {e}")

            if 'Fault' in response:
                raise UserException(f"Error processing endpoint {endpoint}, action {action}, with data {data}:"
                                    f" {response['Fault']}")

    def process_with_logging(self, client, csv_path, endpoint, action, batches):
        self.errors_table = self.create_out_table_definition(self.ERRORS_TABLE_NAME,
                                                             primary_key=[],
                                                             incremental=True, write_always=True)
        with open(self.errors_table.full_path, 'w') as ef:
            writer = csv.DictWriter(ef, fieldnames=["id", "endpoint", "action", "body", "error", "ts"])
            writer.writeheader()

            for batch in batches:
                data = self.get_batch(csv_path, batch)
                entries = create_entries(endpoint, action, data)
                response = client.send(endpoint, entries)

                if response:
                    error_to_write = {
                        "id": data[0]['Id'], # noqa
                        "endpoint": endpoint,
                        "action": action,
                        "body": str(entries),
                        "error": str(response.get('Fault')),
                        "ts": self.start_ts
                    }
                    logging.warning(error_to_write)
                    writer.writerow(error_to_write)

    @staticmethod
    def check_columns(endpoint: str, action: str, csv_path: str) -> None:
        try:
            expected_columns[endpoint][action]
        except KeyError:
            raise UserException(f"Unsupported action for endpoint {endpoint}: {action}")

        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            csv_columns = reader.fieldnames

            missing_columns = [col for col in expected_columns[endpoint][action] if col not in csv_columns]
            if missing_columns:
                raise UserException(f"Missing columns in input table for endpoint {endpoint}: {missing_columns}")

    @staticmethod
    def get_batches(csv_file_path: str) -> list:
        unique_combinations = set()

        with open(csv_file_path, mode='r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                id_value = str(row['Id']) # noqa
                entity_name_value = str(row['EntityName']) # noqa
                unique_combinations.add((id_value, entity_name_value))

        return list(unique_combinations)

    @staticmethod
    def get_batch(csv_file_path: str, unique_combination: list[tuple[str, str]]) -> list[str]:
        batch = []

        with open(csv_file_path, mode='r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                id_value = str(row['Id']) # noqa
                entity_name_value = str(row['EntityName']) # noqa

                if (id_value, entity_name_value) == unique_combination:
                    batch.append(row)
        return batch

    def refresh_and_save_quickbooks_token(self, client: QuickbooksClient):
        """Uses Quickbooks client to get new tokens and saves them using API."""
        client.refresh_access_token()

        if self.environment_variables.token:
            self.save_new_oauth_token(client.refresh_token)
        else:
            logging.warning("No storage token found. Skipping token save at the beginning of the run.")

    def get_refresh_token(self, oauth) -> str:

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
        logging.info("Saving new token to state using Keboola API.")

        try:
            encrypted_refresh_token = self.encrypt(refresh_token)
        except requests.exceptions.RequestException:
            logging.warning("Encrypt API is unavailable. Skipping token save at the beginning of the run.")
            return

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
        response.raise_for_status()
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
