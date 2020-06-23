# Assumptions for local testing
* Run under a virtual environment
* Run pip install -r requirements.txt
* Have a local test file structure like:
>   ./test-map-data-sandbox-us-west-2/secrets/passwords.json
* Secrets file should have the following element:
>   { 
>       "mysql_password": "password_here"
>   }
* FIID 52124 db is setup in your local mysql db
* AWS Sudo/magic script has been run

# Assumptions for running in the wild
Environment Vars:

* HOST
* PORT
* USERNAME
* SNS_SLACK_ARN
* SECRETS_BUCKET
* SECRETS_KEY
* DATABASE_PASSWORD_KEY
* DEST_BUCKET
* DEST_PREFIX
* MAILING_LIST_SOURCE_BUCKET
* MAILING_LIST_PREFIX

# Running Tests
> pytest