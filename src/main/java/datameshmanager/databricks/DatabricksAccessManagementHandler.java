package datameshmanager.databricks;

import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.error.platform.NotFound;
import com.databricks.sdk.service.catalog.PermissionsChange;
import com.databricks.sdk.service.catalog.Privilege;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.SecurableType;
import com.databricks.sdk.service.catalog.UpdatePermissions;
import com.databricks.sdk.service.catalog.UpdatePermissionsResponse;
import com.databricks.sdk.service.iam.ComplexValue;
import com.databricks.sdk.service.iam.Group;
import com.databricks.sdk.service.iam.ListAccountGroupsRequest;
import com.databricks.sdk.service.iam.ServicePrincipal;
import datameshmanager.sdk.DataMeshManagerClient;
import datameshmanager.sdk.DataMeshManagerEventHandler;
import datameshmanager.sdk.client.ApiException;
import datameshmanager.sdk.client.model.Access;
import datameshmanager.sdk.client.model.AccessActivatedEvent;
import datameshmanager.sdk.client.model.AccessDeactivatedEvent;
import datameshmanager.sdk.client.model.DataProduct;
import datameshmanager.sdk.client.model.DataProductOutputPortsInner;
import datameshmanager.sdk.client.model.DataProductOutputPortsInnerServer;
import datameshmanager.sdk.client.model.Team;
import datameshmanager.sdk.client.model.TeamMembersInner;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksAccessManagementHandler implements DataMeshManagerEventHandler {

  private static final Logger log = LoggerFactory.getLogger(DatabricksAccessManagementHandler.class);

  private final DataMeshManagerClient client;
  private final WorkspaceClient workspaceClient;
  private final AccountClient accountClient;

  public DatabricksAccessManagementHandler(
      DataMeshManagerClient client,
      WorkspaceClient workspaceClient,
      AccountClient accountClient) {
    this.client = client;
    this.workspaceClient = workspaceClient;
    this.accountClient = accountClient;
  }

  @Override
  public void onAccessActivatedEvent(AccessActivatedEvent event) {
    log.info("Processing AccessActivatedEvent {}", event.getId());
    var access = getAccess(event.getId());
    if( access == null) {
      log.info("Access {} not found, skip granting permissions", event.getId());
      return;
    }
    if (!isApplicable(access)) {
      log.info("Access {} is not applicable for Databricks access management", access.getId());
      return;
    }
    if (!isActive(access)) {
      log.info("Access {} is not active, skip granting permissions", access.getId());
      return;
    }
    grantPermissions(access);
  }

  @Override
  public void onAccessDeactivatedEvent(AccessDeactivatedEvent event) {
    log.info("Processing AccessDeactivatedEvent {}", event.getId());
    var access = getAccess(event.getId());
    if (access == null) {
      log.info("Access {} not found, skip revoking permissions", event.getId());
      return;
    }
    if (!isApplicable(access)) {
      log.info("Access {} is not applicable for Databricks access management", access.getId());
      return;
    }
    revokePermissions(access);
  }

  private boolean isApplicable(Access access) {
    var dataProductId = access.getProvider().getDataProductId();
    var dataProduct = getDataProduct(dataProductId);
    var outputPortId = access.getProvider().getOutputPortId();
    var outputPort = getOutputPort(dataProduct, outputPortId);
    var server = outputPort.getServer();
    if (outputPort.getType() == null || !Objects.equals(outputPort.getType().toLowerCase(), "databricks")) {
      log.info("Output port type is not databricks for dataProductId {}, outputPortId: {}", dataProductId, outputPortId);
      return false;
    }
    if (server == null) {
      log.warn("Server is undefined for dataProductId {}, outputPortId: {}", dataProductId, outputPortId);
      return false;
    }
    String configHost = this.workspaceClient.config().getHost();
    String serverHost = server.get("host");
    boolean hostnamesMatch = Objects.equals(URI.create(configHost).getHost(), URI.create(serverHost).getHost());

    if (!hostnamesMatch) {
      log.info("Hostnames do not match: datameshmanager.client.databricks.host={} and outputport.server.host={}", configHost, serverHost);
      return false;
    }

    return true;
  }

  private boolean isActive(Access access) {
    return Objects.equals(access.getInfo().getActive(), Boolean.TRUE);
  }

  void grantPermissions(Access access) {
    var dataProductId = access.getProvider().getDataProductId();
    var dataProduct = getDataProduct(dataProductId);
    var outputPortId = access.getProvider().getOutputPortId();
    var outputPort = getOutputPort(dataProduct, outputPortId);
    var schemaFullName = getSchemaFullName(outputPort, dataProductId);
    var accessGroupName = "access-" + access.getId();

    var accessGroup = createDatabricksGroup(accessGroupName);

    switch (consumerType(access)) {
      case DATA_PRODUCT -> {
        // create a service principal for the consumer data product
        log.info("Creating service principal for consumer data product {}", access.getConsumer().getDataProductId());
        var consumerDataProductServicePrincipalId = createDatabricksServiceProvider(access.getConsumer().getDataProductId());
        addMemberToGroup(accessGroup, consumerDataProductServicePrincipalId);

        // also add the consumer team to the access group
        log.info("Adding consumer team to access group {}", accessGroupName);
        var consumerTeam = getConsumerTeam(access.getConsumer().getTeamId());
        var consumerTeamGroupName = "team-" + consumerTeam.getId();
        var teamGroup = createDatabricksGroup(consumerTeamGroupName);
        addMembersToGroup(teamGroup, getMemberEmailAddresses(consumerTeam));
        addMemberToGroup(accessGroup, teamGroup.getId());
      }
      case TEAM -> {
        var consumerTeam = getConsumerTeam(access.getConsumer().getTeamId());
        var consumerTeamGroupId = "team-" + consumerTeam.getId();
        var teamGroup = createDatabricksGroup(consumerTeamGroupId);
        addMembersToGroup(teamGroup, getMemberEmailAddresses(consumerTeam));
        addMemberToGroup(accessGroup, consumerTeamGroupId);
      }
      case USER -> {
        var userId = access.getConsumer().getUserId();
        addMemberToGroup(accessGroup, userId);
      }
    }

    grantSchemaPermissions(schemaFullName, accessGroup.getDisplayName());

    // TODO: update access resource in Data Mesh Manager with logs
  }

  /**
   * Revoking permissions means simply deleting the Databricks group for this Access resource.
   * Databricks will take a few seconds until the permissions are also removed in UI from the secured object (i.e. schema).
   */
  private void revokePermissions(Access access) {
    String accessGroupName = "access-" + access.getId();
    Optional<Group> accessGroupOptional = getGroupByName(accessGroupName);
    if (accessGroupOptional.isEmpty()) {
      log.info("Group {} does not exist or was already deleted", accessGroupName);
      return;
    }
    log.info("Deleting access group {} for access {}", accessGroupName, access.getId());
    accountClient.groups().delete(accessGroupOptional.get().getId());
    log.info("Access group {} deleted", accessGroupName);
  }

  /**
   * Create an account group if it does not exist.
   * Workspace groups are legacy and cannot be used for unity catalog access control.
   */
  private Group createDatabricksGroup(String groupName) {
    var group = getGroupByName(groupName);
    if (group.isPresent()) {
      log.info("Group {} already exists", groupName);
      return group.get();
    }
    log.info("Creating group {}", groupName);
    var newGroup = new Group()
        .setDisplayName(groupName);
    Group createdGroup = accountClient.groups().create(newGroup);
    log.info("Created group ID={}, Name={}", createdGroup.getId(), createdGroup.getDisplayName());
    return createdGroup;
  }

  private Optional<Group> getGroupByName(String groupName) {
    Iterable<Group> groups = accountClient.groups()
        .list(new ListAccountGroupsRequest().setFilter("displayName eq \"" + groupName + "\""));
    return groups.iterator().hasNext() ? Optional.of(groups.iterator().next()) : Optional.empty();
  }

  private Optional<Group> getGroupById(String groupId) {
    try {
      return Optional.of(accountClient.groups().get(groupId));
    } catch (NotFound e) {
      return Optional.empty();
    }
  }

  private void addMemberToGroup(Group group, String principalId) {
    addMembersToGroup(group, List.of(principalId));
  }

  private void addMembersToGroup(Group group, List<String> principalIds) {
    var group1 = getGroupById(group.getId()).orElseThrow(() -> {
      log.error("Group {} does not exist", group.getId());
      return new IllegalStateException("Group " + group.getId() + " does not exist");
    });
    var changed = false;
    for (String principalId : principalIds) {
      if (group1.getMembers() != null && group1.getMembers().stream().noneMatch(m -> m.getValue().equals(principalId))) {
        log.info("Adding member {} to group {}", principalId, group.getId());
        group1.getMembers().add(new ComplexValue().setValue(principalId));
        changed = true;
      } else {
        log.info("Member {} already in group {}", principalId, group.getId());
      }
    }
    if (changed) {
      log.info("Updating group {}", group.getId());
      accountClient.groups().update(group1);
    }
  }

  private Team getConsumerTeam(String teamId) {
    return client.getTeamsApi().getTeam(teamId);
  }

  private static List<String> getMemberEmailAddresses(Team consumerTeam) {
    if (consumerTeam.getMembers() == null) {
      return Collections.emptyList();
    }
    return consumerTeam.getMembers().stream().map(TeamMembersInner::getEmailAddress).toList();
  }


  private ConsumerType consumerType(Access access) {
    //noinspection ConstantValue
    if (access.getConsumer().getDataProductId() != null) {
      return ConsumerType.DATA_PRODUCT;
    } else if (access.getConsumer().getTeamId() != null) {
      return ConsumerType.TEAM;
    } else if (access.getConsumer().getUserId() != null) {
      return ConsumerType.USER;
    }
    throw new IllegalArgumentException("Unknown consumer type");
  }

  enum ConsumerType {
    DATA_PRODUCT,
    TEAM,
    USER
  }

  private String createDatabricksServiceProvider(String dataProductId) {
    DataProduct dataProduct = getDataProduct(dataProductId);
    String servicePrincipalId = getServicePrincipalId(dataProduct);

    ServicePrincipal servicePrincipal = workspaceClient.servicePrincipals().get(servicePrincipalId);

    if (servicePrincipal == null) {
      log.info("Creating service principal for data product {}", dataProductId);
      servicePrincipal = workspaceClient.servicePrincipals().create(
          new ServicePrincipal()
              .setId(servicePrincipalId)
              .setDisplayName("Data Product " + dataProduct.getInfo().getTitle())
              .setExternalId(dataProductId)
              .setActive(true)
      );
    }

    return servicePrincipal.getId();
  }

  private static String getServicePrincipalId(DataProduct dataProduct) {
    // TODO if a custom field mapping is configured, use it as the service principal id
    return "dataproduct-" + dataProduct.getId();
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
    boolean hostnamesMatch = Objects.equals(URI.create(configHost).getHost(), URI.create(serverHost).getHost());
    if (!hostnamesMatch) {
      log.error("The host names don't match: datameshmanager.client.databricks.host={} and outputport.server.host{}", configHost,
          serverHost);
      throw new RuntimeException(
          "The host names don't match: datameshmanager.client.databricks.host=" + configHost + " and outputport.server.host" + serverHost);
    }
  }


  public void grantSchemaPermissions(String schemaFullName, String principal) {

    // verify that the schema exists in databricks
    SchemaInfo schemaInfo = workspaceClient.schemas().get(schemaFullName);
    if (schemaInfo == null) {
      log.error("Schema {} not found in Databricks", schemaFullName);
      return;
    }

    log.info("Granting SELECT permission to principal {} on schema {}", principal, schemaFullName);
    UpdatePermissionsResponse grantedPermissions = workspaceClient.grants().update(
        new UpdatePermissions()
        .setSecurableType(SecurableType.SCHEMA.name())
        .setFullName(schemaFullName)
        .setChanges(Collections.singleton(
            new PermissionsChange()
                .setPrincipal(principal)
                .setAdd(Collections.singleton(Privilege.SELECT))
        )));
    log.info("Granted permissions: {}", grantedPermissions);

    // TODO return log information
  }

  private Access getAccess(String accessId) {
    try {
      return client.getAccessApi().getAccess(accessId);
    } catch (ApiException e) {
      if(e.getCode() == 404) {
        log.info("Access {} not found", accessId);
        return null;
      } else {
        log.error("Error getting access", e);
        throw e;
      }
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
