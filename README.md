Data Mesh Manager Agent for Databricks
===

The agent for databricks is a Spring Boot application that uses the [datamesh-manager-sdk](https://github.com/datamesh-manager/datamesh-manager-sdk) internally, and is available as a ready-to-use Docker image [datameshmanager/datamesh-manager-agent-databricks](https://hub.docker.com/repository/docker/datameshmanager/datamesh-manager-agent-databricks) to be deployed in your environment.

## Features

- **Asset Synchronization**: Sync tables and schemas of the Unity catalog to the Data Mesh Manager as Assets. 
- **Access Management**: Listen for AccessActivated and AccessDeactivated events in the Data Mesh Manager and grants access on Databricks to the data consumer.

## Usage

Start the agent using Docker. You must pass the API keys as environment variables.

```
docker run \
  -e DATAMESHMANAGER_CLIENT_APIKEY='insert-api-key-here' \
  -e DATAMESHMANAGER_CLIENT_DATABRICKS_HOST='https://dbc-xxxxxx.cloud.databricks.com/' \
  -e DATAMESHMANAGER_CLIENT_DATABRICKS_TOKEN='your-access-token' \
  datameshmanager/datamesh-manager-agent-databricks:latest
```

## Configuration

| Environment Variable                                                         | Default Value                      | Description                                                                            |
|------------------------------------------------------------------------------|------------------------------------|----------------------------------------------------------------------------------------|
| `DATAMESHMANAGER_CLIENT_HOST`                                                | `https://api.datamesh-manager.com` | Base URL of the Data Mesh Manager API.                                                 |
| `DATAMESHMANAGER_CLIENT_APIKEY`                                              |                                    | API key for authenticating requests to the Data Mesh Manager.                          |
| `DATAMESHMANAGER_CLIENT_DATABRICKS_HOST`                                     |                                    | Databricks workspace host URL in the form of `https://dbc-xxxxxx.cloud.databricks.com/`.                                         |
| `DATAMESHMANAGER_CLIENT_DATABRICKS_TOKEN`                                    |                                    | Personal access token for authenticating with Databricks.                              |
| `DATAMESHMANAGER_CLIENT_DATABRICKS_ACCESSMANAGEMENT_AGENTID`                 | `databricks-access-management`     | Identifier for the Databricks access management agent.                                 |
| `DATAMESHMANAGER_CLIENT_DATABRICKS_ACCESSMANAGEMENT_ENABLED`                 | `true`                             | Indicates whether Databricks access management is enabled.                             |
| `DATAMESHMANAGER_CLIENT_DATABRICKS_ACCESSMANAGEMENT_MAPPING_DATAPRODUCT_CUSTOMFIELD` | `databricksServicePrincipal`       | Custom field mapping for Databricks service principals in data products.               |
| `DATAMESHMANAGER_CLIENT_DATABRICKS_ACCESSMANAGEMENT_MAPPING_TEAM_CUSTOMFIELD`       | `databricksServicePrincipal`       | Custom field mapping for Databricks service principals in teams.                       |
| `DATAMESHMANAGER_CLIENT_DATABRICKS_ASSETS_AGENTID`                           | `databricks-assets`                | Identifier for the Databricks assets agent.                                            |
| `DATAMESHMANAGER_CLIENT_DATABRICKS_ASSETS_ENABLED`                           | `true`                             | Indicates whether Databricks asset tracking is enabled.                                |
| `DATAMESHMANAGER_CLIENT_DATABRICKS_ASSETS_POLLINTERVAL`                      | `PT5S`                             | Polling interval for Databricks asset updates, in ISO 8601 duration format.            |
| `DATAMESHMANAGER_CLIENT_DATABRICKS_ASSETS_TABLES_ALLOWLIST`                  | `*`                                | List of allowed tables for Databricks asset tracking (wildcard `*` allows all tables). |
