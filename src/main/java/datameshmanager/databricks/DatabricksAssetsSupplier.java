package datameshmanager.databricks;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.CatalogType;
import com.databricks.sdk.service.catalog.ListCatalogsRequest;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.TableInfo;
import datameshmanager.sdk.DataMeshManagerAssetsProvider;
import datameshmanager.sdk.DataMeshManagerStateRepository;
import datameshmanager.sdk.client.model.Asset;
import datameshmanager.sdk.client.model.AssetColumnsInner;
import datameshmanager.sdk.client.model.AssetInfo;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksAssetsSupplier implements DataMeshManagerAssetsProvider {

  private static final Logger log = LoggerFactory.getLogger(DatabricksAssetsSupplier.class);

  private final WorkspaceClient workspaceClient;
  private final DataMeshManagerStateRepository dataMeshManagerStateRepository;
  private final DatabricksProperties databricksProperties;

  public DatabricksAssetsSupplier(WorkspaceClient workspaceClient, DataMeshManagerStateRepository dataMeshManagerStateRepository,
      DatabricksProperties databricksProperties) {
    this.workspaceClient = workspaceClient;
    this.dataMeshManagerStateRepository = dataMeshManagerStateRepository;
    this.databricksProperties = databricksProperties;
  }

  @Override
  public void fetchAssets(AssetCallback assetCallback) {
    final var databricksLastUpdatedAt = getLastUpdatedAt();
    var databricksLastUpdatedAtThisRunMax = databricksLastUpdatedAt;

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

        schemaToAsset(schema, databricksLastUpdatedAt).ifPresent(assetCallback::onAssetUpdated);

        var tables = workspaceClient.tables().list(schema.getCatalogName(), schema.getName());
        for (var table : tables) {
          tableToAsset(table, databricksLastUpdatedAt).ifPresent(assetCallback::onAssetUpdated);

          databricksLastUpdatedAtThisRunMax = Math.max(databricksLastUpdatedAtThisRunMax, table.getUpdatedAt());
        }
      }
    }

    setLastUpdatedAt(databricksLastUpdatedAtThisRunMax);
  }

  private Long getLastUpdatedAt() {
    Map<String, Object> state = dataMeshManagerStateRepository.getState();
    return (Long) state.getOrDefault("lastUpdatedAt", 0L);
  }

  private void setLastUpdatedAt(Long databricksLastUpdatedAtThisRunMax) {
    Map<String, Object> state = Map.of("lastUpdatedAt", databricksLastUpdatedAtThisRunMax);
    dataMeshManagerStateRepository.saveState(state);
  }


  protected Optional<Asset> schemaToAsset(SchemaInfo schema, Long databricksLastUpdatedAt) {

    if (!includeSchema(schema)) {
      log.debug("Skipping schema {}", schema.getFullName());
      return Optional.empty();
    }

    if (alreadySynchronized(schema, databricksLastUpdatedAt)) {
      log.info("Schema {} already synchronized", schema.getFullName());
      return Optional.empty();
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
        .putPropertiesItem("host", databricksProperties.workspace().host())
        .putPropertiesItem("catalog", schema.getCatalogName())
        .putPropertiesItem("catalogType", schema.getCatalogType())
        .putPropertiesItem("schema", schema.getName())
        .putPropertiesItem("updatedAt", schema.getUpdatedAt().toString());

    return Optional.of(asset);
  }

  protected Optional<Asset> tableToAsset(TableInfo table, Long databricksLastUpdatedAt) {
    if (!includeTable(table)) {
      log.debug("Skipping table {}", table.getFullName());
      return Optional.empty();
    }

    if (alreadySynchronized(table, databricksLastUpdatedAt)) {
      log.info("Table {} already synchronized", table.getFullName());
      return Optional.empty();
    }

    log.info("Synchronizing table {}", table.getFullName());

    Asset asset = new Asset()
        .id(table.getTableId())
        .info(new AssetInfo()
            .name(table.getFullName())
            .source("unity")
            .qualifiedName(table.getFullName())
            .type(table.getTableType().name())
            .status("active")
            .description(table.getComment()))
        .putPropertiesItem("host", databricksProperties.workspace().host())
        .putPropertiesItem("catalog", table.getCatalogName())
        .putPropertiesItem("schema", table.getSchemaName())
        .putPropertiesItem("table", table.getName())
        .putPropertiesItem("tableType", table.getTableType())
        .putPropertiesItem("updatedAt", table.getUpdatedAt().toString());

    if (table.getColumns() != null) {
      for (var column : table.getColumns()) {
        asset.addColumnsItem(new AssetColumnsInner()
            .name(column.getName())
            .type(column.getTypeText())
            .description(column.getComment()));
      }
    }

    return Optional.of(asset);
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

  private boolean alreadySynchronized(SchemaInfo schema, Long databricksLastUpdatedAt) {
    return databricksLastUpdatedAt >= schema.getUpdatedAt();
  }

  private boolean alreadySynchronized(TableInfo table, Long databricksLastUpdatedAt) {
    return databricksLastUpdatedAt >= table.getUpdatedAt();
  }


}
