package datameshmanager.databricks;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.PermissionsChange;
import com.databricks.sdk.service.catalog.PermissionsList;
import com.databricks.sdk.service.catalog.Privilege;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.SecurableType;
import com.databricks.sdk.service.catalog.UpdatePermissions;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DatabricksPermissionsService {

  private static final Logger log = LoggerFactory.getLogger(DatabricksPermissionsService.class);

  private final WorkspaceClient workspaceClient;

  public DatabricksPermissionsService(WorkspaceClient workspaceClient) {
    this.workspaceClient = workspaceClient;
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
