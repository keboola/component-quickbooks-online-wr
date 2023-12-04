import csv
import json
import logging
import os
import datetime

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
        client.refresh_access_token()

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
                writer = csv.DictWriter(f, fieldnames=["id", "error"])
                for row in reader:
                    try:
                        data = json.loads(row['data'])
                    except json.decoder.JSONDecodeError as e:
                        raise UserException(f"Cannot decode row {row['id']}: {e}")
                    error = function(data)
                    if error:
                        error_to_write = {
                            "id": row['id'],
                            "error": error
                        }
                        writer.writerow(error_to_write)

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
                logging.debug("Loaded tokens from statefile.")
            else:
                logging.debug("Using tokens from oAuth.")
        else:
            logging.warning("No timestamp found in statefile. Using oAuth tokens.")

        return refresh_token


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
