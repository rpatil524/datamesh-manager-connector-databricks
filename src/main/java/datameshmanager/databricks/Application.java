package datameshmanager.databricks;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import datameshmanager.sdk.DataMeshManagerAssetsSynchronizer;
import datameshmanager.sdk.DataMeshManagerClient;
import datameshmanager.sdk.DataMeshManagerEventListener;
import datameshmanager.sdk.DataMeshManagerStateRepository;
import datameshmanager.sdk.DataMeshManagerStateRepositoryInMemory;
import datameshmanager.sdk.DataMeshManagerStateRepositoryRemote;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "datameshmanager")
@ConfigurationPropertiesScan("datameshmanager")
@EnableScheduling
public class Application {

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @Bean
  public WorkspaceClient workspaceClient(DatabricksProperties properties) {
    var host = properties.host();
    var token = properties.token();
    var databricksConfig = new DatabricksConfig().setHost(host).setToken(token);
    return new WorkspaceClient(databricksConfig);
  }

  @Bean
  public DataMeshManagerStateRepository dataMeshManagerStateRepository(DataMeshManagerClient client) {
    return new DataMeshManagerStateRepositoryRemote(client);
  }

  @Bean(initMethod = "start", destroyMethod = "stop")
  @ConditionalOnProperty(value = "datameshmanager.client.databricks.accessmanagement.enabled", havingValue = "true")
  public DataMeshManagerEventListener dataMeshManagerEventListener(DataMeshManagerClient client, DatabricksProperties databricksProperties,
      WorkspaceClient workspaceClient, DataMeshManagerStateRepository stateRepository) {
    var agentid = databricksProperties.accessmanagement().agentid();
    var eventHandler = new DatabricksAccessManagementHandler(client, databricksProperties, workspaceClient);
    return new DataMeshManagerEventListener(agentid, client, eventHandler, stateRepository);
  }

  @Bean(initMethod = "start", destroyMethod = "stop")
  @ConditionalOnProperty(value = "datameshmanager.client.databricks.assets.enabled", havingValue = "true")
  public DataMeshManagerAssetsSynchronizer dataMeshManagerAssetsSynchronizer(DatabricksProperties databricksProperties,
      DataMeshManagerClient client, WorkspaceClient workspaceClient, DataMeshManagerStateRepository stateRepository) {
    var agentid = databricksProperties.assets().agentid();
    var assetsSupplier = new DatabricksAssetsSupplier(workspaceClient, stateRepository, databricksProperties);
    return new DataMeshManagerAssetsSynchronizer(agentid, client, assetsSupplier);
  }

}
