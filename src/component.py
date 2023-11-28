import csv
import json
import logging
import datetime

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from client import QuickbooksClient

# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_SANDBOX = 'sandbox'
KEY_ENDPOINT = 'endpoint'
KEY_COMPANY_ID = 'company_id'

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
        self.access_token = None

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        oauth = self.configuration.oauth_credentials
        sandbox = self.configuration.parameters.get(KEY_SANDBOX, False)
        company_id = self.configuration.parameters.get(KEY_COMPANY_ID)
        endpoint = self.configuration.parameters.get(KEY_ENDPOINT)

        self.refresh_token, self.access_token = self.get_tokens(oauth)

        in_tables = self.get_input_tables_definitions()
        in_table = in_tables[0] if in_tables else None

        if not in_table:
            raise UserException("No input table found, exiting.")

        self.client = QuickbooksClient(company_id, self.refresh_token, self.access_token, oauth, sandbox)

        with open(in_table.full_path, 'r') as f:
            reader = csv.DictReader(f, delimiter=";")

            if endpoint == "journals":
                self.process_journals(reader)
            elif endpoint == "invoices":
                self.process_invoices(reader)
            else:
                raise UserException(f"Unsupported endpoint: {endpoint}")

        self.write_state_file({
            "tokens":
                {"ts": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                 "#refresh_token": self.refresh_token,
                 "#access_token": self.access_token}
        })

    def process_journals(self, reader: csv.DictReader):
        for row in reader:
            entry_json = json.loads(row['entry'])
            self.client.write_journal(entry_json)

    def process_invoices(self, reader: csv.DictReader):
        for row in reader:
            entry_json = json.loads(row['entry'])
            self.client.write_invoice(entry_json)

    def get_tokens(self, oauth):

        try:
            refresh_token = oauth["data"]["refresh_token"]
            access_token = oauth["data"]["access_token"]
        except TypeError:
            raise UserException("OAuth data is not available.")

        statefile = self.get_state_file()
        if statefile.get("tokens", {}).get("ts"):
            ts_oauth = datetime.datetime.strptime(oauth["created"], "%Y-%m-%dT%H:%M:%S.%fZ")
            ts_statefile = datetime.datetime.strptime(statefile["tokens"]["ts"], "%Y-%m-%dT%H:%M:%S.%fZ")

            if ts_statefile > ts_oauth:
                refresh_token = statefile["tokens"].get("#refresh_token")
                access_token = statefile["tokens"].get("#access_token")
                logging.debug("Loaded tokens from statefile.")
            else:
                logging.debug("Using tokens from oAuth.")
        else:
            logging.warning("No timestamp found in statefile. Using oAuth tokens.")

        return refresh_token, access_token


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
