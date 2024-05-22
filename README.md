kds-team.wr-quickbooks-online
=============

Description

**Table of contents:**

[TOC]

Functionality notes
===================

So far only journalentry endpoint is supported with create action. 

Prerequisites
=============

Quickbooks Online Account

Input table with following columns:

    "journalentry": {
        "create": ["Id", "Type", "TxnDate", "PrivateNote", "AccountRefName", "AccountRefValue", "Amount",
                   "Description", "ClassRefName", "DepartmentRefName", "ClassRefValue", "DepartmentRefValue",
                   "EntityName", "DocNumber"]
    }


For more info, please refer to the [Quickbooks Online API](https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/journalentry)


Supported endpoints
===================

-   **Journal Entry, Create** - Create a journal entry in Quickbooks Online https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/journalentry


If you need more endpoints, please submit your request to
[ideas.keboola.com](https://ideas.keboola.com/)

Configuration
=============

TODO

Output
======

TODO

Development
-----------

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to
your custom path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following
command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://bitbucket.org/kds_consulting_team/kds-team.wr-quickbooks-online/src kds-team.wr-quickbooks-online
cd kds-team.wr-quickbooks-online
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers
documentation](https://developers.keboola.com/extend/component/deployment/)
