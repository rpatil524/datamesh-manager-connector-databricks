package datameshmanager.databricks;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.CatalogType;
import com.databricks.sdk.service.catalog.ListCatalogsRequest;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.TableInfo;
import datameshmanager.sdk.DataMeshManagerAgentRegistration;
import datameshmanager.sdk.DataMeshManagerClient;
import datameshmanager.sdk.client.ApiException;
import datameshmanager.sdk.client.model.Asset;
import datameshmanager.sdk.client.model.AssetColumnsInner;
import datameshmanager.sdk.client.model.AssetInfo;
import jakarta.annotation.PreDestroy;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(value = "datameshmanager.client.databricks.assets.enabled", havingValue = "true")
public class DatabricksAssetSynchronizer {

  private static final Logger log = LoggerFactory.getLogger(DatabricksAssetSynchronizer.class);

  private final DataMeshManagerClient client;
  private final DataMeshManagerAgentRegistration agentRegistration;
  private final WorkspaceClient workspaceClient;
  private final DatabricksProperties databricksProperties;

  private Long lastUpdatedAt = 0L;

  public DatabricksAssetSynchronizer(DataMeshManagerClient client, WorkspaceClient workspaceClient,
      DatabricksProperties databricksProperties) {
    this.client = client;
    this.agentRegistration = new DataMeshManagerAgentRegistration(client, databricksProperties.assets().agentid(), "databricks-assets");
    this.workspaceClient = workspaceClient;
    this.databricksProperties = databricksProperties;

    this.agentRegistration.register();
  }

  @Scheduled(fixedDelayString = "${datameshmanager.client.databricks.assets.pollinterval:PT60M}")
  public void synchronizeAssets() {
    this.agentRegistration.up();
    synchronizeDatabricksAssets();
  }

  @PreDestroy
  public void onDestroy() {
    this.agentRegistration.stop();
    log.info("Stopped asset synchronization");
  }


  protected void synchronizeDatabricksAssets() {
    Long databricksLastUpdatedAtThisRunMax = getLastUpdatedAt();
    var catalogs = workspaceClient.catalogs().list(new ListCatalogsRequest());
    for (var catalog : catalogs) {
      if (!includeCatalog(catalog)) {
        continue;
      }

      log.info("Synchronizing catalog {}", catalog);
      var schemas = workspaceClient.schemas().list(catalog.getFullName());
      for (var schema : schemas) {
        if (!includeSchema(schema)) {
          continue;
        }
        handleSchema(schema);
        var tables = workspaceClient.tables().list(schema.getCatalogName(), schema.getName());
        for (var table : tables) {
          handleTable(table);
          databricksLastUpdatedAtThisRunMax = Math.max(databricksLastUpdatedAtThisRunMax, table.getUpdatedAt());
        }

      }
    }

    setLastUpdatedAt(databricksLastUpdatedAtThisRunMax);

  }

  private Long getLastUpdatedAt() {
    return (Long) getState().getOrDefault("lastUpdatedAt", 0L);
  }

  private void setLastUpdatedAt(Long databricksLastUpdatedAtThisRunMax) {
    saveState(Map.of("lastUpdatedAt", databricksLastUpdatedAtThisRunMax));
  }


  protected void handleSchema(SchemaInfo schema) {

    if (!includeSchema(schema)) {
      log.debug("Skipping schema {}", schema.getFullName());
      return;
    }

    if (alreadySynchronized(schema)) {
      log.info("Schema {} already synchronized", schema.getFullName());
      return;
    }

    log.info("Synchronizing schema {}", schema.getFullName());
    Asset asset = new Asset()
        .id(schema.getSchemaId())
        .info(new AssetInfo()
            .name(schema.getFullName())
            .source("unity")
            .qualifiedName(schema.getFullName())
            .type(schema.getCatalogType())
            .status("active")
            .description(schema.getComment()))
        .putPropertiesItem("updatedAt", schema.getUpdatedAt().toString());
    saveAsset(asset);
  }

  protected void handleTable(TableInfo table) {
    if (!includeTable(table)) {
      log.debug("Skipping table {}", table.getFullName());
      return;
    }

    if (alreadySynchronized(table)) {
      log.info("Table {} already synchronized", table.getFullName());
      return;
    }

    log.info("Synchronizing table {}", table.getFullName());

    if (table.getDeletedAt() != null) {
      log.info("Table {} was deleted", table.getFullName());
      deleteAsset(table.getTableId());
      return;
    }

    Asset asset = new Asset()
        .id(table.getTableId())
        .info(new AssetInfo()
            .name(table.getFullName())
            .source("unity")
            .qualifiedName(table.getFullName())
            .type(table.getTableType().name())
            .status("active")
            .description(table.getComment()))
        .putPropertiesItem("updatedAt", table.getUpdatedAt().toString());

    if (table.getColumns() != null) {
      for (var column : table.getColumns()) {
        asset.addColumnsItem(new AssetColumnsInner()
            .name(column.getName())
            .type(column.getTypeText())
            .description(column.getComment()));
      }
    }

    saveAsset(asset);
  }

  protected boolean includeCatalog(CatalogInfo catalog) {
    if (catalog.getCatalogType() == CatalogType.MANAGED_CATALOG) {
      return true;
    }

    return false;
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
    // todo check if resource is already knonwn in Data Mesh Manager
    return this.lastUpdatedAt >= schema.getUpdatedAt();
  }

  private boolean alreadySynchronized(TableInfo table) {
    return this.lastUpdatedAt >= table.getUpdatedAt();
  }

  public Map<String, Object> getState() {
    return Map.of("lastUpdatedAt", this.lastUpdatedAt);
  }

  public void saveState(Map<String, Object> state) {
    this.lastUpdatedAt = (Long) state.get("lastUpdatedAt");
  }

  public void saveAsset(Asset asset) {

    try {
      Asset existingAsset = this.client.getAssetsApi().getAsset(asset.getId());
      if (Objects.deepEquals(asset, existingAsset)) {
        log.info("Asset {} already exists and unchanged", asset.getId());
        return;
      }
    } catch (ApiException e) {
      if (e.getCode() == 404) {
        log.debug("Asset {} does not exist, so continue", asset.getId());
      } else {
        throw e;
      }
    }

    log.info("Saving asset {}", asset.getId());
    client.getAssetsApi().addAsset(asset.getId(), asset);
  }

  public void deleteAsset(String id) {
    log.info("Deleting asset {}", id);
    client.getAssetsApi().deleteAsset(id);
  }

}
