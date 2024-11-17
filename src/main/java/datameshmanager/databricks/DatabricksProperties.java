package datameshmanager.databricks;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datameshmanager.client.databricks")
public record DatabricksProperties(
    String host,
    String token,
    AssetsProperties assets,
    AccessmanagementProperties accessmanagement
) {

  public record AssetsProperties(
      Boolean enabled,
      String agentid
  ) {

  }

  public record AccessmanagementProperties(
      Boolean enabled,
      String agentid,
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
