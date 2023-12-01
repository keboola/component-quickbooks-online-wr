import csv
import json
import logging
import datetime
from typing import Callable

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from client import QuickbooksClient

# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_SANDBOX = 'sandbox'
KEY_ENDPOINT = 'endpoint'
KEY_COMPANY_ID = 'company_id'
KEY_FAIL_ON_ERROR = 'fail_on_error'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_COMPANY_ID, KEY_ENDPOINT]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    BASE_URL = "https://quickbooks.api.intuit.com"

    def __init__(self):
        super().__init__()
        self.client: QuickbooksClient
        self.refresh_token = None
        self.result_table = None

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        oauth = self.configuration.oauth_credentials
        sandbox = self.configuration.parameters.get(KEY_SANDBOX, False)
        company_id = self.configuration.parameters.get(KEY_COMPANY_ID)
        endpoint = self.configuration.parameters.get(KEY_ENDPOINT)[0]
        fail_on_error = self.configuration.parameters.get(KEY_FAIL_ON_ERROR, False)

        self.refresh_token = self.get_refresh_token(oauth)

        in_tables = self.get_input_tables_definitions()
        in_table = in_tables[0] if in_tables else None

        if not in_table:
            raise UserException("No input table found, exiting.")

        client = QuickbooksClient(company_id, self.refresh_token, oauth, sandbox, fail_on_error)
        client.refresh_access_token()

        with open(in_table.full_path, 'r') as f:
            reader = csv.DictReader(f)

            if endpoint == "journals":
                self.process_endpoint(client, reader, client.write_journal, fail_on_error)
            elif endpoint == "invoices":
                self.process_endpoint(client, reader, client.write_invoice, fail_on_error)
            else:
                raise UserException(f"Unsupported endpoint: {endpoint}")

        self.write_manifest(self.result_table)

        self.write_state_file({
            "token":
                {"ts": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                 "#refresh_token": client.refresh_token}
        })

    def process_endpoint(self, client, reader: csv.DictReader, function: Callable, fail_on_error: bool):
        if not fail_on_error:
            for row in reader:
                data = json.loads(row['data'])
                client.write_journal(data)
        else:
            self.result_table = self.create_out_table_definition("results", primary_key=["id"], incremental=True)
            with open(self.result_table.full_path, 'w') as f:
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
            ts_statefile = datetime.datetime.strptime(statefile["tokens"]["ts"], "%Y-%m-%dT%H:%M:%S.%fZ")

            if ts_statefile > ts_oauth:
                refresh_token = statefile["tokens"].get("#refresh_token")
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
