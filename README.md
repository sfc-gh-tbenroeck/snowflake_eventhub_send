# EventHub Integration with Snowflake

This repository contains code to facilitate sending and receiving messages between Snowflake and Azure EventHub. Please replace any placeholder values (indicated by `__VARIABLE__`) with actual values for your configuration.

## Overview of Files

- `.env_template`: Template for environment variables specific to EventHub configurations.
- `eventhub_receiver/receive.py`: Python script to receive messages from EventHub.
- `snowflake/eventhub_send_batch.sql`: SQL script for configuring Snowflake to send batch messages to EventHub.
- `.vscode/launch.json`: Configuration for VS Code to load the `.env` file for debugging.

## Getting Started

### Snowflake Setup for Security Integration

The following steps are required for Snowflake to communicate securely with Azure EventHub, regardless of whether SAS or OAuth is used for authentication.

#### Step 1: Define Network Rules
Define network rules in Snowflake for outbound traffic to EventHub:

```sql
CREATE OR REPLACE NETWORK RULE az_eventhubs_network_rule
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('__NAMESPACE__.servicebus.windows.net');
```

#### Step 2: Create Security Integration in Snowflake
The security integration allows Snowflake to access Azure EventHub. This integration will be referenced later during function creation, based on the chosen authentication method.

```sql
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION az_eventhubs_integration
ALLOWED_NETWORK_RULES = (az_eventhubs_network_rule)
ENABLED = true;
```

### Authentication Methods

You have two options for authenticating Snowflake to Azure EventHub: **Shared Access Signature (SAS)** or **AAD OAuth with aService Principal**.

#### Option 1: SAS Authentication
1. **Create a Secret for SAS**:
   Store the SAS credentials securely in Snowflake as a secret:

   ```sql
   CREATE OR REPLACE SECRET az_eventhubs_sharedkey
   TYPE = password
   USERNAME = '__KEY_NAME__'
   PASSWORD = '__KEY_VALUE__';
   ```

2. **Update Security Integration** to include SAS:
   ```sql
   ALTER EXTERNAL ACCESS INTEGRATION az_eventhubs_integration
   SET ALLOWED_AUTHENTICATION_SECRETS = (az_eventhubs_sharedkey);
   ```

#### Option 2: OAuth Authentication
1. **Create a Service Principal (SP) with Azure CLI**:
   ```bash
   az ad sp create-for-rbac -n az_snowflake_eventhub_sp
   ```

   **Console Output:**
   ```json
   {
     "appId": "__a1a1a1a1a-1a1a1-11a1-11aa-11111aa1a111__",
     "displayName": "az_snowflake_eventhub_sp",
     "password": "__secure_password_from_az_cli__",
     "tenant": "__00b0b00bb-00b0-00bb-b000-bb0bb0bb00b0__"
   }
   ```

2. **Set Up OAuth Integration in Snowflake**:
   Create a security integration to use OAuth:

   ```sql
   CREATE SECURITY INTEGRATION az_snowflake_eventhub_sp
     TYPE = API_AUTHENTICATION
     AUTH_TYPE = OAUTH2
     ENABLED = TRUE
     OAUTH_TOKEN_ENDPOINT = 'https://login.microsoftonline.com/__TENANT_ID__/oauth2/v2.0/token'
     OAUTH_CLIENT_ID = '__CLIENT_ID__'
     OAUTH_CLIENT_SECRET = '__CLIENT_SECRET__'
     OAUTH_ALLOWED_SCOPES = ('https://__NAMESPACE__.servicebus.windows.net/.default');
   ```

   Alternatively, you can follow the [Azure documentation for creating an Enterprise Application](https://learn.microsoft.com/en-us/rest/api/eventhub/get-azure-active-directory-token).

3. **Update Security Integration** to include OAuth:
   ```sql
   ALTER EXTERNAL ACCESS INTEGRATION az_eventhubs_integration
   SET ALLOWED_AUTHENTICATION_SECRETS = (az_snowflake_eventhub_sp);
   ```

### 4. Create Functions to Send Messages to EventHub

Based on the chosen authentication method, set up the appropriate function in Snowflake to send batch messages to EventHub.

#### Function for SAS Authentication

If using SAS authentication, create the following function:

```sql
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
```

#### Function for OAuth Authentication

If using OAuth, create the following function:

```sql
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
```

### 5. Testing the Functions
The messages sent to EventHub should follow a specific format, either as a **simple array of strings** or as **an array of objects with `Body` as the key**.

#### Option 1: Array of Strings
If the messages are simple string values, you can use an array of strings directly. This is suitable for sending basic information such as timestamps, account names, etc.

**Example:**
```sql
SELECT az_eventhubs_sendbatch_sas('__NAMESPACE__', '__EVENT_HUB_NAME__', ARRAY_CONSTRUCT(current_timestamp(), current_account(), current_date()));
```

#### Option 2: Array of Objects with `Body` Key
For more complex messages, especially when working with data from a query, each message should be formatted as an object with a `Body` key. This allows you to include richer content or structured data within each message body.

**Example with Query Results:**
Here’s an example that queries recent entries from the `query_history` table and constructs a batch of messages with each entry wrapped as a `Body` object:
```sql
SELECT ARRAY_AGG(
        object_construct('Body', to_json(message))
    ) AS messageBatch,
    az_eventhubs_sendbatch_sp('__NAMESPACE__', '__EVENT_HUB_NAME__', messageBatch)
FROM (SELECT  {* EXCLUDE query_text} message FROM  snowflake.account_usage.query_history LIMIT 5);
```

Each record in the `query_history` table is converted to JSON and wrapped in an object with `Body` as the key, making it suitable for sending complex data structures to EventHub in a single batch.


### Running a Local EventHub Receiver

This example uses a SAS connection string for quick local testing. For additional authentication options, refer to the [EventHub documentation](https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/eventhub/azure-eventhub).

1. **Copy** `.env_template` to `.env`:
   ```bash
   cp .env_template .env
   ```

2. **Update** `.env` with your specific EventHub configurations:
   ```plaintext
   EVENT_HUB_CONNECTION_STR="Endpoint=sb://__NAMESPACE__.servicebus.windows.net/;SharedAccessKeyName=__KEY_NAME__;SharedAccessKey=__KEY_VALUE__"
   EVENT_HUB_CONSUMER_GROUP="$Default"
   EVENT_HUB_NAME="__EVENT_HUB_NAME__"
   ```

3. **Loading Environment Variables**
   - To load `.env` in your Python environment automatically, you can use the provided `launch.json` configuration in VS Code. It will load `.env` variables when debugging:
     - `.vscode/launch.json`:
       ```json
       {
           "version": "0.2.0",
           "configurations": [
               {
                   "name": "Python Debugger: Current File",
                   "type": "debugpy",
                   "request": "launch",
                   "program": "${file}",
                   "console": "integratedTerminal",
                   "envFile": "${workspaceFolder}/.env"
               }
           ]
       }
       ```
   - Alternatively, if you are running the script directly in the terminal, ensure `.env` is sourced or variables are manually exported.

4. **Run `python receiver.py` or debug from VSCode**

    Example receiver.py output:
    ```plaintext
    --- Received new event batch with 3 events ----------------------------
        ---> Event 1: 2024-10-31 11:04:53.806 -0700
        ---> Event 2: WP48969
        ---> Event 3: 2024-10-31
    ------------------------------------------------------------------------------------
    --- Received new event batch with 5 events ----------------------------
        ---> Event 1: {"BYTES_DELETED":0,"BYTES_READ_FROM_RESULT":0,"BYTES_SCANNED":0,"BYTES_SENT_OVER_THE_NETWORK":0,"BYTES_SPILLED_TO_LOCAL_STORAGE":0,"BYTES_SPILLED_TO_REMOTE_STORAGE":0,"BYTES_WRITTEN":0,"BYTES_WRITTEN_TO_RESULT":0,"CHILD_QUERIES_WAIT_TIME":0,"COMPILATION_TIME":145,"CREDITS_USED_CLOUD_SERVICES":0.000000000000000e+00,"END_TIME":"2023-11-21 23:09:26.604 -0800","EXECUTION_STATUS":"SUCCESS","EXECUTION_TIME":206,"EXTERNAL_FUNCTION_TOTAL_INVOCATIONS":0,"EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES":0,"EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS":0,"EXTERNAL_FUNCTION_TOTAL_SENT_BYTES":0,"EXTERNAL_FUNCTION_TOTAL_SENT_ROWS":0,"INBOUND_DATA_TRANSFER_BYTES":0,"IS_CLIENT_GENERATED_STATEMENT":true,"LIST_EXTERNAL_FILES_TIME":0,"OUTBOUND_DATA_TRANSFER_BYTES":0,"PARTITIONS_SCANNED":0,"PARTITIONS_TOTAL":0,"PERCENTAGE_SCANNED_FROM_CACHE":0.000000000000000e+00,"QUERY_ACCELERATION_BYTES_SCANNED":0,"QUERY_ACCELERATION_PARTITIONS_SCANNED":0,"QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR":0,"QUERY_HASH":"10d452946a19594eb7f0b53c72ec0c7d","QUERY_HASH_VERSION":2,"QUERY_ID":"01b07e8d-0101-ff68-0001-0fc2006908c2","QUERY_PARAMETERIZED_HASH":"10d452946a19594eb7f0b53c72ec0c7d","QUERY_PARAMETERIZED_HASH_VERSION":1,"QUERY_TAG":"","QUERY_TYPE":"REMOVE_FILES","QUEUED_OVERLOAD_TIME":0,"QUEUED_PROVISIONING_TIME":0,"QUEUED_REPAIR_TIME":0,"RELEASE_VERSION":"7.41.0","ROLE_NAME":"WORKSHEETS_APP_RL","ROLE_TYPE":"ROLE","ROWS_DELETED":0,"ROWS_INSERTED":0,"ROWS_UNLOADED":0,"ROWS_UPDATED":0,"ROWS_WRITTEN_TO_RESULT":0,"SESSION_ID":298800882555118,"START_TIME":"2023-11-21 23:09:26.253 -0800","TOTAL_ELAPSED_TIME":351,"TRANSACTION_BLOCKED_TIME":0,"TRANSACTION_ID":0,"USER_NAME":"WORKSHEETS_APP_USER"}
        ---> Event 2: {"BYTES_DELETED":0,"BYTES_READ_FROM_RESULT":0,"BYTES_SCANNED":0,"BYTES_SENT_OVER_THE_NETWORK":0,"BYTES_SPILLED_TO_LOCAL_STORAGE":0,"BYTES_SPILLED_TO_REMOTE_STORAGE":0,"BYTES_WRITTEN":0,"BYTES_WRITTEN_TO_RESULT":0,"CHILD_QUERIES_WAIT_TIME":0,"COMPILATION_TIME":58,"CREDITS_USED_CLOUD_SERVICES":0.000000000000000e+00,"END_TIME":"2023-11-21 23:09:32.308 -0800","EXECUTION_STATUS":"SUCCESS","EXECUTION_TIME":20,"EXTERNAL_FUNCTION_TOTAL_INVOCATIONS":0,"EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES":0,"EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS":0,"EXTERNAL_FUNCTION_TOTAL_SENT_BYTES":0,"EXTERNAL_FUNCTION_TOTAL_SENT_ROWS":0,"INBOUND_DATA_TRANSFER_BYTES":0,"IS_CLIENT_GENERATED_STATEMENT":true,"LIST_EXTERNAL_FILES_TIME":0,"OUTBOUND_DATA_TRANSFER_BYTES":0,"PARTITIONS_SCANNED":0,"PARTITIONS_TOTAL":0,"PERCENTAGE_SCANNED_FROM_CACHE":0.000000000000000e+00,"QUERY_ACCELERATION_BYTES_SCANNED":0,"QUERY_ACCELERATION_PARTITIONS_SCANNED":0,"QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR":0,"QUERY_HASH":"e669c428e896863cf6b1b4a940b8dbe7","QUERY_HASH_VERSION":2,"QUERY_ID":"01b07e8d-0101-ff53-0001-0fc20069237e","QUERY_PARAMETERIZED_HASH":"e669c428e896863cf6b1b4a940b8dbe7","QUERY_PARAMETERIZED_HASH_VERSION":1,"QUERY_TAG":"","QUERY_TYPE":"REMOVE_FILES","QUEUED_OVERLOAD_TIME":0,"QUEUED_PROVISIONING_TIME":0,"QUEUED_REPAIR_TIME":0,"RELEASE_VERSION":"7.41.0","ROLE_NAME":"WORKSHEETS_APP_RL","ROLE_TYPE":"ROLE","ROWS_DELETED":0,"ROWS_INSERTED":0,"ROWS_UNLOADED":0,"ROWS_UPDATED":0,"ROWS_WRITTEN_TO_RESULT":0,"SESSION_ID":298800882561562,"START_TIME":"2023-11-21 23:09:32.230 -0800","TOTAL_ELAPSED_TIME":78,"TRANSACTION_BLOCKED_TIME":0,"TRANSACTION_ID":0,"USER_NAME":"WORKSHEETS_APP_USER"}
        ---> Event 3: {"BYTES_DELETED":0,"BYTES_READ_FROM_RESULT":0,"BYTES_SCANNED":0,"BYTES_SENT_OVER_THE_NETWORK":0,"BYTES_SPILLED_TO_LOCAL_STORAGE":0,"BYTES_SPILLED_TO_REMOTE_STORAGE":0,"BYTES_WRITTEN":0,"BYTES_WRITTEN_TO_RESULT":0,"CHILD_QUERIES_WAIT_TIME":0,"COMPILATION_TIME":42,"CREDITS_USED_CLOUD_SERVICES":0.000000000000000e+00,"END_TIME":"2023-11-21 23:09:25.179 -0800","EXECUTION_STATUS":"SUCCESS","EXECUTION_TIME":47,"EXTERNAL_FUNCTION_TOTAL_INVOCATIONS":0,"EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES":0,"EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS":0,"EXTERNAL_FUNCTION_TOTAL_SENT_BYTES":0,"EXTERNAL_FUNCTION_TOTAL_SENT_ROWS":0,"INBOUND_DATA_TRANSFER_BYTES":0,"IS_CLIENT_GENERATED_STATEMENT":true,"LIST_EXTERNAL_FILES_TIME":0,"OUTBOUND_DATA_TRANSFER_BYTES":0,"PARTITIONS_SCANNED":0,"PARTITIONS_TOTAL":0,"PERCENTAGE_SCANNED_FROM_CACHE":0.000000000000000e+00,"QUERY_ACCELERATION_BYTES_SCANNED":0,"QUERY_ACCELERATION_PARTITIONS_SCANNED":0,"QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR":0,"QUERY_HASH":"2f99409139e5269985fbe99b0da484ff","QUERY_HASH_VERSION":2,"QUERY_ID":"01b07e8d-0101-ff68-0001-0fc2006908be","QUERY_PARAMETERIZED_HASH":"2f99409139e5269985fbe99b0da484ff","QUERY_PARAMETERIZED_HASH_VERSION":1,"QUERY_TAG":"","QUERY_TYPE":"REMOVE_FILES","QUEUED_OVERLOAD_TIME":0,"QUEUED_PROVISIONING_TIME":0,"QUEUED_REPAIR_TIME":0,"RELEASE_VERSION":"7.41.0","ROLE_NAME":"WORKSHEETS_APP_RL","ROLE_TYPE":"ROLE","ROWS_DELETED":0,"ROWS_INSERTED":0,"ROWS_UNLOADED":0,"ROWS_UPDATED":0,"ROWS_WRITTEN_TO_RESULT":0,"SESSION_ID":298800882555110,"START_TIME":"2023-11-21 23:09:25.090 -0800","TOTAL_ELAPSED_TIME":89,"TRANSACTION_BLOCKED_TIME":0,"TRANSACTION_ID":0,"USER_NAME":"WORKSHEETS_APP_USER"}
        ---> Event 4: {"BYTES_DELETED":0,"BYTES_READ_FROM_RESULT":0,"BYTES_SCANNED":0,"BYTES_SENT_OVER_THE_NETWORK":0,"BYTES_SPILLED_TO_LOCAL_STORAGE":0,"BYTES_SPILLED_TO_REMOTE_STORAGE":0,"BYTES_WRITTEN":0,"BYTES_WRITTEN_TO_RESULT":0,"CHILD_QUERIES_WAIT_TIME":0,"COMPILATION_TIME":186,"CREDITS_USED_CLOUD_SERVICES":0.000000000000000e+00,"END_TIME":"2023-11-21 23:09:28.954 -0800","EXECUTION_STATUS":"SUCCESS","EXECUTION_TIME":15,"EXTERNAL_FUNCTION_TOTAL_INVOCATIONS":0,"EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES":0,"EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS":0,"EXTERNAL_FUNCTION_TOTAL_SENT_BYTES":0,"EXTERNAL_FUNCTION_TOTAL_SENT_ROWS":0,"INBOUND_DATA_TRANSFER_BYTES":0,"IS_CLIENT_GENERATED_STATEMENT":true,"LIST_EXTERNAL_FILES_TIME":0,"OUTBOUND_DATA_TRANSFER_BYTES":0,"PARTITIONS_SCANNED":0,"PARTITIONS_TOTAL":0,"PERCENTAGE_SCANNED_FROM_CACHE":0.000000000000000e+00,"QUERY_ACCELERATION_BYTES_SCANNED":0,"QUERY_ACCELERATION_PARTITIONS_SCANNED":0,"QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR":0,"QUERY_HASH":"caf4afb16cbf9b97dc4f8dd6e950763c","QUERY_HASH_VERSION":2,"QUERY_ID":"01b07e8d-0101-ff53-0001-0fc20069236e","QUERY_PARAMETERIZED_HASH":"caf4afb16cbf9b97dc4f8dd6e950763c","QUERY_PARAMETERIZED_HASH_VERSION":1,"QUERY_TAG":"","QUERY_TYPE":"REMOVE_FILES","QUEUED_OVERLOAD_TIME":0,"QUEUED_PROVISIONING_TIME":0,"QUEUED_REPAIR_TIME":0,"RELEASE_VERSION":"7.41.0","ROLE_NAME":"WORKSHEETS_APP_RL","ROLE_TYPE":"ROLE","ROWS_DELETED":0,"ROWS_INSERTED":0,"ROWS_UNLOADED":0,"ROWS_UPDATED":0,"ROWS_WRITTEN_TO_RESULT":0,"SESSION_ID":298800882561554,"START_TIME":"2023-11-21 23:09:28.753 -0800","TOTAL_ELAPSED_TIME":201,"TRANSACTION_BLOCKED_TIME":0,"TRANSACTION_ID":0,"USER_NAME":"WORKSHEETS_APP_USER"}
        ---> Event 5: {"BYTES_DELETED":0,"BYTES_READ_FROM_RESULT":0,"BYTES_SCANNED":0,"BYTES_SENT_OVER_THE_NETWORK":0,"BYTES_SPILLED_TO_LOCAL_STORAGE":0,"BYTES_SPILLED_TO_REMOTE_STORAGE":0,"BYTES_WRITTEN":0,"BYTES_WRITTEN_TO_RESULT":0,"CHILD_QUERIES_WAIT_TIME":0,"COMPILATION_TIME":405,"CREDITS_USED_CLOUD_SERVICES":0.000000000000000e+00,"END_TIME":"2023-11-21 23:02:06.411 -0800","EXECUTION_STATUS":"SUCCESS","EXECUTION_TIME":101,"EXTERNAL_FUNCTION_TOTAL_INVOCATIONS":0,"EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES":0,"EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS":0,"EXTERNAL_FUNCTION_TOTAL_SENT_BYTES":0,"EXTERNAL_FUNCTION_TOTAL_SENT_ROWS":0,"INBOUND_DATA_TRANSFER_BYTES":0,"IS_CLIENT_GENERATED_STATEMENT":true,"LIST_EXTERNAL_FILES_TIME":0,"OUTBOUND_DATA_TRANSFER_BYTES":0,"PARTITIONS_SCANNED":0,"PARTITIONS_TOTAL":0,"PERCENTAGE_SCANNED_FROM_CACHE":0.000000000000000e+00,"QUERY_ACCELERATION_BYTES_SCANNED":0,"QUERY_ACCELERATION_PARTITIONS_SCANNED":0,"QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR":0,"QUERY_HASH":"5b09b629c8751a1e12e0512c949a53cc","QUERY_HASH_VERSION":2,"QUERY_ID":"01b07e86-0101-ff53-0001-0fc20069233a","QUERY_PARAMETERIZED_HASH":"5b09b629c8751a1e12e0512c949a53cc","QUERY_PARAMETERIZED_HASH_VERSION":1,"QUERY_TAG":"","QUERY_TYPE":"REMOVE_FILES","QUEUED_OVERLOAD_TIME":0,"QUEUED_PROVISIONING_TIME":0,"QUEUED_REPAIR_TIME":0,"RELEASE_VERSION":"7.41.0","ROLE_NAME":"WORKSHEETS_APP_RL","ROLE_TYPE":"ROLE","ROWS_DELETED":0,"ROWS_INSERTED":0,"ROWS_UNLOADED":0,"ROWS_UPDATED":0,"ROWS_WRITTEN_TO_RESULT":0,"SESSION_ID":298800882555054,"START_TIME":"2023-11-21 23:02:05.905 -0800","TOTAL_ELAPSED_TIME":506,"TRANSACTION_BLOCKED_TIME":0,"TRANSACTION_ID":0,"USER_NAME":"WORKSHEETS_APP_USER"}
    ------------------------------------------------------------------------------------
    ```
