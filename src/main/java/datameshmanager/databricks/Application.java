package datameshmanager.databricks;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import datameshmanager.sdk.DataMeshManagerAssetsSynchronizer;
import datameshmanager.sdk.DataMeshManagerClient;
import datameshmanager.sdk.DataMeshManagerClientProperties;
import datameshmanager.sdk.DataMeshManagerEventListener;
import datameshmanager.sdk.DataMeshManagerStateRepositoryInMemory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Service;

@SpringBootApplication(scanBasePackages = "datameshmanager")
@ConfigurationPropertiesScan("datameshmanager")
@EnableScheduling
public class Application {

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @Bean
  public WorkspaceClient workspaceClient(DatabricksProperties properties) {
    DatabricksConfig databricksConfig = new DatabricksConfig()
        .setHost(properties.host())
        .setToken(properties.token());
    return new WorkspaceClient(databricksConfig);
  }

  @Bean(initMethod = "start", destroyMethod = "stop")
  @ConditionalOnProperty(value = "datameshmanager.client.databricks.accessmanagement.enabled", havingValue = "true")
  public DataMeshManagerEventListener dataMeshManagerEventListener(DataMeshManagerClient client,
      DataMeshManagerClientProperties clientProperties,
      DatabricksProperties databricksProperties, // TODO difference between client and databricks properties?
      WorkspaceClient workspaceClient) {
    return new DataMeshManagerEventListener(clientProperties.id(),
        new DatabricksAccessManagementHandler(client, databricksProperties, workspaceClient), client,
        new DataMeshManagerStateRepositoryInMemory());
  }

  @Bean(initMethod = "start", destroyMethod = "stop")
  @ConditionalOnProperty(value = "datameshmanager.client.databricks.assets.enabled", havingValue = "true")
  public DataMeshManagerAssetsSynchronizer dataMeshManagerAssetsSynchronizer(
      DatabricksProperties databricksProperties, DataMeshManagerClient dataMeshManagerClient, WorkspaceClient workspaceClient) {
    return new DataMeshManagerAssetsSynchronizer(databricksProperties.assets().agentid(), dataMeshManagerClient,
        new DatabricksAssetSupplier(workspaceClient, new DataMeshManagerStateRepositoryInMemory(), databricksProperties));
  }

}
