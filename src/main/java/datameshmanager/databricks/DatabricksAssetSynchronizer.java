package datameshmanager.databricks;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.TableInfo;
import datameshmanager.sdk.DataMeshManagerClient;
import datameshmanager.sdk.client.ApiException;
import datameshmanager.sdk.client.model.Asset;
import datameshmanager.sdk.client.model.AssetColumnsInner;
import datameshmanager.sdk.client.model.AssetInfo;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(value = "datameshmanager.client.databricks.assets.enabled", havingValue = "true")
public class DatabricksAssetSynchronizer implements CommandLineRunner {

  private static final Logger log = LoggerFactory.getLogger(DatabricksAccessManagement.class);

  private final DataMeshManagerClient client;
  private final DatabricksProperties databricksProperties;
  private final WorkspaceClient workspaceClient;

  private Duration pollInterval = Duration.ofSeconds(5);
  private Map<String, Long> lastUpdatedAt = new HashMap<>();

  public DatabricksAssetSynchronizer(DataMeshManagerClient client,
      DatabricksProperties databricksProperties, WorkspaceClient workspaceClient) {
    this.client = client;
    this.databricksProperties = databricksProperties;
    this.workspaceClient = workspaceClient;
  }


  @Override
  public void run(String... args) throws Exception {

    while (true) {
      synchronizeDatabricksAssets();

      try {
        log.info("Sleeping for {} to make the next call", pollInterval);
        Thread.sleep(pollInterval.toMillis());
      } catch (InterruptedException e) {
        break;
      }

    }
  }

  protected void synchronizeDatabricksAssets() throws ApiException {
    var catalogs = List.of("datamesh_manager_agent_databricks_test3");
    for (var catalog : catalogs) {
      log.info("Synchronizing catalog {}", catalog);
      var schemas = workspaceClient.schemas().list(catalog);
      for (var schema : schemas) {
        if (!includeSchema(schema)) {
          continue;
        }
        updateSchema(schema);
        for (var table : workspaceClient.tables().list(schema.getCatalogName(), schema.getName())) {
          updateTable(table);
        }
      }
    }
  }

  protected void updateSchema(SchemaInfo schema) throws ApiException {
    if (alreadySynchronized(schema)) {
      log.info("Schema {} already synchronized", schema.getFullName());
      return;
    }

    if (!includeSchema(schema)) {
      log.debug("Skipping schema {}", schema.getFullName());
      return;
    }

    log.info("Synchronizing schema {}", schema.getFullName());

    client.getAssetsApi().addAsset(
        schema.getSchemaId(),
        new Asset()
            .id(schema.getSchemaId())
            .info(new AssetInfo()
                .name(schema.getFullName())
                .source("unity")
                .qualifiedName(schema.getFullName())
                .type(schema.getCatalogType())
                .status("active")
                .description(schema.getComment()))
            .putPropertiesItem("updatedAt", schema.getUpdatedAt().toString())
    );

    lastUpdatedAt.put(schema.getFullName(), schema.getUpdatedAt());
  }

  protected void updateTable(TableInfo table) throws ApiException {
    if (!includeTable(table)) {
      log.debug("Skipping table {}", table.getFullName());
      return;
    }

    if (alreadySynchronized(table)) {
      log.info("Table {} already synchronized", table.getFullName());
      return;
    }

    log.info("Synchronizing table {}", table.getFullName());
    client.getAssetsApi().addAsset(
        table.getTableId(),
        new Asset()
            .id(table.getTableId())
            .info(new AssetInfo()
                .name(table.getFullName())
                .source("unity")
                .qualifiedName(table.getFullName())
                .type(table.getTableType().name())
                .status("active")
                .description(table.getComment()))
            .columns(table.getColumns().stream().map(columnInfo ->
                    new AssetColumnsInner()
                        .name(columnInfo.getName())
                        .type(columnInfo.getTypeText())
                        .description(columnInfo.getComment())
                ).toList()
            )
            .putPropertiesItem("updatedAt", table.getUpdatedAt().toString())
    );
    lastUpdatedAt.put(table.getFullName(), table.getUpdatedAt());
  }

  protected boolean includeSchema(SchemaInfo schema) {
    if (Objects.equals(schema.getName(), "information_schema")) {
      return false;
    }
    return true;
  }

  protected boolean includeTable(TableInfo table) {
    return true;
  }

  private boolean alreadySynchronized(SchemaInfo schema) {
    Long lastUpdated = lastUpdatedAt.get(schema.getFullName());
    return lastUpdated != null && lastUpdated >= schema.getUpdatedAt();
  }

  private boolean alreadySynchronized(TableInfo table) {
    Long lastUpdated = lastUpdatedAt.get(table.getFullName());
    return lastUpdated != null && lastUpdated >= table.getUpdatedAt();
  }

}
