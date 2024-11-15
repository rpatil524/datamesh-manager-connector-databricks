package datameshmanager.databricks;

import com.databricks.sdk.WorkspaceClient;
import datameshmanager.sdk.DataMeshManagerClient;
import datameshmanager.sdk.DataMeshManagerClientProperties;
import datameshmanager.sdk.DataMeshManagerEventHandler;
import datameshmanager.sdk.DataMeshManagerEventListener;
import datameshmanager.sdk.DataMeshManagerStateRepositoryInMemory;
import datameshmanager.sdk.client.ApiException;
import datameshmanager.sdk.client.model.Access;
import datameshmanager.sdk.client.model.AccessActivatedEvent;
import datameshmanager.sdk.client.model.AccessDeactivatedEvent;
import datameshmanager.sdk.client.model.DataProduct;
import datameshmanager.sdk.client.model.DataProductOutputPortsInner;
import datameshmanager.sdk.client.model.DataProductOutputPortsInnerServer;
import jakarta.annotation.PreDestroy;
import java.net.URI;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(value = "datameshmanager.client.databricks.accessmanagement.enabled", havingValue = "true")
public class DatabricksAccessManagement implements CommandLineRunner {

  private static final Logger log = LoggerFactory.getLogger(DatabricksAccessManagement.class);
  private final DataMeshManagerClient client;
  private final DataMeshManagerClientProperties clientProperties;
  private final DatabricksProperties databricksProperties;
  private final DatabricksPermissionsService permissionsService;
  private final WorkspaceClient workspaceClient;

  private DataMeshManagerEventListener eventListener = null;

  public DatabricksAccessManagement(
      DataMeshManagerClient client,
      DataMeshManagerClientProperties clientProperties, DatabricksProperties databricksProperties,
      DatabricksPermissionsService permissionsService,
      WorkspaceClient workspaceClient) {
    this.client = client;
    this.clientProperties = clientProperties;
    this.databricksProperties = databricksProperties;
    this.permissionsService = permissionsService;
    this.workspaceClient = workspaceClient;
  }

  @Override
  public void run(String... args) throws Exception {
    System.out.println("Running datamesh-manager-agent-databricks for access management");

    DataMeshManagerEventHandler eventHandler = new DataMeshManagerEventHandler() {

      @Override
      public void onAccessActivatedEvent(AccessActivatedEvent event) {
        log.info("Processing AccessActivatedEvent {}", event.getId());
        grantPermissions(event.getId());
      }

      @Override
      public void onAccessDeactivatedEvent(AccessDeactivatedEvent event) {
        // TODO revoke permissions in Databricks
      }
    };

    // TODO use DataMeshManager as state repository
    var stateRepository = new DataMeshManagerStateRepositoryInMemory();
    this.eventListener = new DataMeshManagerEventListener(clientProperties.id(), eventHandler, client, stateRepository);
    this.eventListener.start();

  }

  @PreDestroy
  public void onDestroy() {
    log.info("Stopping event listener");
    this.eventListener.stop();
  }

  void grantPermissions(String accessId) {
    var access = getAccess(accessId);
    var dataProductId = access.getProvider().getDataProductId();
    var dataProduct = getDataProduct(dataProductId);
    var outputPortId = access.getProvider().getOutputPortId();
    var outputPort = getOutputPort(dataProduct, outputPortId);
    var schemaFullName = getSchemaFullName(outputPort, dataProductId);
    var principal = getConsumerPrincipal(access);

    permissionsService.grantSchemaPermissionsToPrincipal(schemaFullName, principal);

    // TODO: update access resource in Data Mesh Manager with logs
  }

  protected String getConsumerPrincipal(Access access) {
    // Ignore always true warning, as the OpenAPI generator is not aware of oneOf
    if (access.getConsumer().getDataProductId() != null) {
      DataProduct consumerDataProduct = getDataProduct(access.getConsumer().getDataProductId());
      String principalField = databricksProperties.accessmanagement().mapping().dataproduct().customfield();
      if (principalField == null) {
        log.error("Configuration datameshmanager.client.databricks.accessmanagement.mapping.dataproduct.customfield undefined");
        throw new RuntimeException("Configuration datameshmanager.client.databricks.accessmanagement.mapping.dataproduct.customfield undefined");
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

}
