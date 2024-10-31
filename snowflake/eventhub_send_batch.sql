use database __DATABASE__;
use schema __SCHEMA__;
use warehouse __WAREHOUSE__;

CREATE OR REPLACE NETWORK RULE az_eventhubs_network_rule
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('__NAMESPACE__.servicebus.windows.net');

-- Create Secret for Username & Password
CREATE OR REPLACE SECRET az_eventhubs_sharedkey
TYPE = password
USERNAME = '__KEY_NAME__'
PASSWORD = '__KEY_VALUE__';

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION az_eventhubs_integration
ALLOWED_NETWORK_RULES = (az_eventhubs_network_rule)
ALLOWED_AUTHENTICATION_SECRETS = (az_eventhubs_sharedkey)
ENABLED = true;

CREATE OR REPLACE FUNCTION az_eventhubs_sendbatch_sas(namespace STRING, eventhub STRING, messages ARRAY)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'send_batch'
EXTERNAL_ACCESS_INTEGRATIONS = (az_eventhubs_integration)
PACKAGES = ('snowflake-snowpark-python', 'requests')
SECRETS = ('cred' = az_eventhubs_sharedkey)
AS
$$
import _snowflake
import requests
import json
import time
import urllib.parse
import hmac
import hashlib
import base64

def send_batch(namespace, eventhub, messages):
    # Generate SAS token for Authorization
    sharedkey_object = _snowflake.get_username_password('cred');
    sas_token = generate_sas_token(namespace, eventhub, sharedkey_object.username, sharedkey_object.password)

    # Define the Event Hub endpoint
    endpoint = f"https://{namespace}.servicebus.windows.net/{eventhub}/messages?api-version=2014-01"

    # Prepare the headers for Event Hub API request
    headers = {
        "Content-Type": "application/vnd.microsoft.servicebus.json",
        "Authorization": sas_token
    }

    # Prepare the JSON payload or the batch messages
    payload = []

    for message in messages:
        # If message is a dict and already has "Body", validate it
        if isinstance(message, dict) and "Body" in message:
            # Retain only "Body" and optional "UserProperties"
            validated_message = {"Body": message["Body"]}
            if "UserProperties" in message:
                validated_message["UserProperties"] = message["UserProperties"]
            payload.append(validated_message)
        else:
            # If message is a string or lacks "Body", wrap it as a dict with "Body"
            payload.append({"Body": message})

    # Send the POST request to Event Hub
    response = requests.post(endpoint, json=payload, headers=headers)

    # Check response and return result
    if response.status_code == 201:
        return "Batch sent successfully!"
    else:
        return f"Failed to send batch. Status code: {response.status_code}, Response: {response.text}"

def generate_sas_token(sb_name, eh_name, sas_name, sas_value):
    """
    Generates a SAS token for Azure Event Hubs REST API calls.
    """
    # Construct the URI
    uri = urllib.parse.quote_plus(f"https://{sb_name}.servicebus.windows.net/{eh_name}")

    # Convert the SAS key to bytes
    sas = sas_value.encode('utf-8')

    # Define token expiration, 300 = 5 minutes
    expiry = str(int(time.time() + 300))

    # Create the signature string
    string_to_sign = (uri + '\n' + expiry).encode('utf-8')
    signed_hmac_sha256 = hmac.new(sas, string_to_sign, hashlib.sha256)
    signature = urllib.parse.quote(base64.b64encode(signed_hmac_sha256.digest()))

    # Return the SAS token
    return f"SharedAccessSignature sr={uri}&sig={signature}&se={expiry}&skn={sas_name}"
$$
;

SELECT az_eventhubs_sendbatch_sas('__NAMESPACE__', '__EVENT_HUB_NAME__', ARRAY_CONSTRUCT(current_timestamp(), current_account(), current_date()));

SELECT ARRAY_AGG(
        object_construct('Body', to_json(message))
    ) messageBatch
    , az_eventhubs_sendbatch_sas('__NAMESPACE__', '__EVENT_HUB_NAME__', messageBatch)
FROM (SELECT  {* EXCLUDE query_text} message FROM  snowflake.account_usage.query_history LIMIT 5)
;

/*
EventHub Azure documentation walks through creating an Enterprise Application.
https://learn.microsoft.com/en-us/rest/api/eventhub/get-azure-active-directory-token

Create a RBAC SP with the Azure CLI
az ad sp create-for-rbac -n az_snowflake_eventhub_sp

{
  "appId": "__a1a1a1a1a-1a1a1-11a1-11aa-11111aa1a111__",
  "displayName": "az_snowflake_eventhub_sp",
  "password": "__secure_password_from_az_cli__",
  "tenant": "__00b0b00bb-00b0-00bb-b000-bb0bb0bb00b0__"
}

 */

CREATE SECURITY INTEGRATION az_snowflake_eventhub_sp
  TYPE = API_AUTHENTICATION
  AUTH_TYPE = OAUTH2
  ENABLED = TRUE
  OAUTH_TOKEN_ENDPOINT = 'https://login.microsoftonline.com/__00b0b00bb-00b0-00bb-b000-bb0bb0bb00b0__/oauth2/v2.0/token'
  OAUTH_CLIENT_AUTH_METHOD = CLIENT_SECRET_POST
  OAUTH_CLIENT_ID = '__a1a1a1a1a-1a1a1-11a1-11aa-11111aa1a111__'
  OAUTH_CLIENT_SECRET = '__secure_password_from_az_cli__'
  OAUTH_GRANT = 'client_credentials'
  OAUTH_ALLOWED_SCOPES = ( 'https://__NAMESPACE__.servicebus.windows.net/.default' )
;

CREATE OR REPLACE SECRET az_snowflake_eventhub_sp
  TYPE = OAUTH2
  API_AUTHENTICATION = az_snowflake_eventhub_sp;


CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION az_eventhubs_integration
ALLOWED_NETWORK_RULES = (az_eventhubs_network_rule)
ALLOWED_AUTHENTICATION_SECRETS = (az_eventhubs_sharedkey)
ENABLED = true;ALTER EXTERNAL ACCESS INTEGRATION az_eventhubs_integration
SET ALLOWED_AUTHENTICATION_SECRETS = (az_eventhubs_sharedkey, az_snowflake_eventhub_sp)
;

CREATE OR REPLACE FUNCTION az_eventhubs_sendbatch_sp(namespace STRING, eventhub STRING, messages ARRAY)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'send_batch'
EXTERNAL_ACCESS_INTEGRATIONS = (az_eventhubs_integration)
PACKAGES = ('snowflake-snowpark-python', 'requests')
SECRETS = ('cred' = az_snowflake_eventhub_sp)
AS
$$
import _snowflake
import requests
import json
import time
import urllib.parse
import hmac
import hashlib
import base64

def send_batch(namespace, eventhub, messages):
    token = _snowflake.get_oauth_access_token('cred')

    # Define the Event Hub endpoint
    endpoint = f"https://{namespace}.servicebus.windows.net/{eventhub}/messages?api-version=2014-01"

    # Prepare the headers for Event Hub API request
    headers = {
        "Content-Type": "application/vnd.microsoft.servicebus.json",
        "Authorization": f"Bearer {token}"
    }

    # Prepare the JSON payload or the batch messages
    payload = []

    for message in messages:
        # If message is a dict and already has "Body", validate it
        if isinstance(message, dict) and "Body" in message:
            # Retain only "Body" and optional "UserProperties"
            validated_message = {"Body": message["Body"]}
            if "UserProperties" in message:
                validated_message["UserProperties"] = message["UserProperties"]
            payload.append(validated_message)
        else:
            # If message is a string or lacks "Body", wrap it as a dict with "Body"
            payload.append({"Body": message})

    # Send the POST request to Event Hub
    response = requests.post(endpoint, json=payload, headers=headers)

    # Check response and return result
    if response.status_code == 201:
        return "Batch sent successfully!"
    else:
        return f"Failed to send batch. Status code: {response.status_code}, Response: {response.text}"

$$
;


SELECT az_eventhubs_sendbatch_sp('__NAMESPACE__', '__EVENT_HUB_NAME__', ARRAY_CONSTRUCT(current_timestamp(), current_account(), current_date()));

SELECT ARRAY_AGG(
        object_construct('Body', to_json(message))
    ) messageBatch
    , az_eventhubs_sendbatch_sp('__NAMESPACE__', '__EVENT_HUB_NAME__', messageBatch)
FROM (SELECT  {* EXCLUDE query_text} message FROM  snowflake.account_usage.query_history LIMIT 5)
;
