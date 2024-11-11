package datameshmanager.databricks;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication(scanBasePackages = "datameshmanager")
@ConfigurationPropertiesScan("datameshmanager")
public class Application {

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

}
