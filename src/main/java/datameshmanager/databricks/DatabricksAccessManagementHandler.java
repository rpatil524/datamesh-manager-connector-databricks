package datameshmanager.databricks;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.PermissionsChange;
import com.databricks.sdk.service.catalog.PermissionsList;
import com.databricks.sdk.service.catalog.Privilege;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.SecurableType;
import com.databricks.sdk.service.catalog.UpdatePermissions;
import datameshmanager.sdk.DataMeshManagerClient;
import datameshmanager.sdk.DataMeshManagerEventHandler;
import datameshmanager.sdk.client.ApiException;
import datameshmanager.sdk.client.model.Access;
import datameshmanager.sdk.client.model.AccessActivatedEvent;
import datameshmanager.sdk.client.model.AccessDeactivatedEvent;
import datameshmanager.sdk.client.model.DataProduct;
import datameshmanager.sdk.client.model.DataProductOutputPortsInner;
import datameshmanager.sdk.client.model.DataProductOutputPortsInnerServer;
import java.net.URI;
import java.util.Collections;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksAccessManagementHandler implements DataMeshManagerEventHandler {

  private static final Logger log = LoggerFactory.getLogger(DatabricksAccessManagementHandler.class);

  private final DataMeshManagerClient client;
  private final DatabricksProperties databricksProperties;
  private final WorkspaceClient workspaceClient;

  public DatabricksAccessManagementHandler(
      DataMeshManagerClient client,
      DatabricksProperties databricksProperties,
      WorkspaceClient workspaceClient) {
    this.client = client;
    this.databricksProperties = databricksProperties;
    this.workspaceClient = workspaceClient;
  }

  @Override
  public void onAccessActivatedEvent(AccessActivatedEvent event) {
    log.info("Processing AccessActivatedEvent {}", event.getId());
    grantPermissions(event.getId());
  }

  @Override
  public void onAccessDeactivatedEvent(AccessDeactivatedEvent event) {
    // TODO revoke permissions in Databricks
  }

  void grantPermissions(String accessId) {
    var access = getAccess(accessId);
    var dataProductId = access.getProvider().getDataProductId();
    var dataProduct = getDataProduct(dataProductId);
    var outputPortId = access.getProvider().getOutputPortId();
    var outputPort = getOutputPort(dataProduct, outputPortId);
    var schemaFullName = getSchemaFullName(outputPort, dataProductId);
    var principal = getConsumerPrincipal(access);

    grantSchemaPermissionsToPrincipal(schemaFullName, principal);

    // TODO: update access resource in Data Mesh Manager with logs
  }

  protected String getConsumerPrincipal(Access access) {
    // Ignore always true warning, as the OpenAPI generator is not aware of oneOf
    if (access.getConsumer().getDataProductId() != null) {
      DataProduct consumerDataProduct = getDataProduct(access.getConsumer().getDataProductId());
      String principalField = databricksProperties.accessmanagement().mapping().dataproduct().customfield();
      if (principalField == null) {
        log.error("Configuration datameshmanager.client.databricks.accessmanagement.mapping.dataproduct.customfield undefined");
        throw new RuntimeException(
            "Configuration datameshmanager.client.databricks.accessmanagement.mapping.dataproduct.customfield undefined");
      }
      return consumerDataProduct.getCustom().get(principalField);
    }
    if (access.getConsumer().getUserId() != null) {
      return access.getConsumer().getUserId();
    }
    return access.getConsumer().getUserId();
  }

  private DataProductOutputPortsInner getOutputPort(DataProduct dataProduct, String outputPortId) {
    return dataProduct.getOutputPorts()
        .stream()
        .filter(outputPort -> outputPort.getId().equals(outputPortId))
        .findFirst().orElse(null);
  }

  private String getSchemaFullName(DataProductOutputPortsInner outputPort, String dataProductId) {
    var server = getServer(outputPort, dataProductId);
    var databricksCatalog = server.get("catalog");
    var databricksSchema = server.get("schema");
    return databricksCatalog + "." + databricksSchema;
  }

  private DataProductOutputPortsInnerServer getServer(DataProductOutputPortsInner outputPort, String dataProductId) {
    var server = outputPort.getServer();
    validateServer(server, dataProductId, outputPort.getId());
    return server;
  }

  private void validateServer(DataProductOutputPortsInnerServer server, String dataProductId, String outputPortId) {
    if (server == null) {
      log.error("Server is null for dataProductId {}, outputPortId: {}", dataProductId, outputPortId);
      throw new RuntimeException("Server does not exist for dataProductId " + dataProductId + " and outputPortId " + outputPortId);
    }

    String configHost = this.workspaceClient.config().getHost();
    String serverHost = server.get("host");
    if (!Objects.equals(URI.create(configHost).getHost(), URI.create(serverHost).getHost())) {
      log.error("The host names don't match: datameshmanager.client.databricks.host={} and outputport.server.host{}", configHost,
          serverHost);
      throw new RuntimeException(
          "The host names don't match: datameshmanager.client.databricks.host=" + configHost + " and outputport.server.host" + serverHost);
    }
  }


  private Access getAccess(String accessId) {
    try {
      return client.getAccessApi().getAccess(accessId);
    } catch (ApiException e) {
      log.error("Error getting access", e);
      throw new RuntimeException(e);
    }
  }

  private DataProduct getDataProduct(String dataProductId) {
    try {
      return client.getDataProductsApi().getDataProduct(dataProductId);
    } catch (ApiException e) {
      log.error("Error getting data product", e);
      throw new RuntimeException(e);
    }
  }

  public void grantSchemaPermissionsToPrincipal(String schemaFullName, String principal) {

//    verify that the schema exists in databricks
    SchemaInfo schemaInfo = workspaceClient.schemas().get(schemaFullName);
    if (schemaInfo == null) {
      log.error("Schema {} not found in databricks", schemaFullName);
      return;
    }

    // create a group for the accessId
    // TODO

    // grant the group access to the schema

    // add consumer principal to the group

    log.info("Granting SELECT permission to principal {} on schema {}", principal, schemaFullName);
    PermissionsList grantedPermissions = workspaceClient.grants().update(new UpdatePermissions()
        .setSecurableType(SecurableType.SCHEMA)
        .setFullName(schemaFullName)
        .setChanges(Collections.singleton(
            new PermissionsChange()
                .setPrincipal(principal)
                .setAdd(Collections.singleton(Privilege.SELECT))
        )));
    log.info("Granted permissions: {}", grantedPermissions);

    // TODO return log information
  }

}
