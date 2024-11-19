package datameshmanager.databricks;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import datameshmanager.sdk.DataMeshManagerAssetsSynchronizer;
import datameshmanager.sdk.DataMeshManagerClient;
import datameshmanager.sdk.DataMeshManagerEventListener;
import datameshmanager.sdk.DataMeshManagerStateRepository;
import datameshmanager.sdk.DataMeshManagerStateRepositoryRemote;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

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
  public DataMeshManagerClient dataMeshManagerClient(
      @Value("${datameshmanager.client.host}") String host,
      @Value("${datameshmanager.client.apikey}") String apiKey) {
    return new DataMeshManagerClient(host, apiKey);
  }

  @Bean
  public DataMeshManagerStateRepository dataMeshManagerStateRepository(DataMeshManagerClient client) {
    return new DataMeshManagerStateRepositoryRemote(client);
  }

  @Bean(destroyMethod = "stop")
  @ConditionalOnProperty(value = "datameshmanager.client.databricks.accessmanagement.enabled", havingValue = "true")
  public DataMeshManagerEventListener dataMeshManagerEventListener(DataMeshManagerClient client, DatabricksProperties databricksProperties,
      WorkspaceClient workspaceClient, DataMeshManagerStateRepository stateRepository, TaskExecutor taskExecutor) {
    var agentid = databricksProperties.accessmanagement().agentid();
    var eventHandler = new DatabricksAccessManagementHandler(client, databricksProperties, workspaceClient);
    var dataMeshManagerEventListener = new DataMeshManagerEventListener(agentid, client, eventHandler, stateRepository);
    taskExecutor.execute(dataMeshManagerEventListener::start);
    return dataMeshManagerEventListener;
  }

  @Bean(destroyMethod = "stop")
  @ConditionalOnProperty(value = "datameshmanager.client.databricks.assets.enabled", havingValue = "true")
  public DataMeshManagerAssetsSynchronizer dataMeshManagerAssetsSynchronizer(DatabricksProperties databricksProperties,
      DataMeshManagerClient client, WorkspaceClient workspaceClient, DataMeshManagerStateRepository stateRepository, TaskExecutor taskExecutor) {
    var agentid = databricksProperties.assets().agentid();
    var assetsSupplier = new DatabricksAssetsSupplier(workspaceClient, stateRepository, databricksProperties);
    var dataMeshManagerAssetsSynchronizer = new DataMeshManagerAssetsSynchronizer(agentid, client, assetsSupplier);
    taskExecutor.execute(dataMeshManagerAssetsSynchronizer::start);
    return dataMeshManagerAssetsSynchronizer;
  }

  @Bean
  public TaskExecutor taskExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(5);
    executor.setMaxPoolSize(10);
    executor.setQueueCapacity(25);
    executor.setThreadNamePrefix("datameshmanager-agent-");
    executor.initialize();
    return executor;
  }

}
