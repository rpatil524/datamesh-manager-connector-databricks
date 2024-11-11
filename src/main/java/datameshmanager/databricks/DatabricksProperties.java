package datameshmanager.databricks;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datameshmanager.client.databricks")
public record DatabricksProperties(
    String host,
    String token,
    AccessmanagementProperties accessmanagement
) {

  public record AccessmanagementProperties(
      Boolean enabled,
      AccessmanagementMappingProperties mapping
  ) {
    public record AccessmanagementMappingProperties(
        AccessmanagementMappingCustomfieldProperties dataproduct,
        AccessmanagementMappingCustomfieldProperties team
    ) {
      public record AccessmanagementMappingCustomfieldProperties(
          String customfield
      ) {
      }
    }
  }


}
