package datameshmanager.databricks;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DatabricksConfiguration {

  @Bean
  public WorkspaceClient workspaceClient(DatabricksProperties properties) {
    DatabricksConfig databricksConfig = new DatabricksConfig()
        .setHost(properties.host())
        .setToken(properties.token());
    return new WorkspaceClient(databricksConfig);
  }

}
