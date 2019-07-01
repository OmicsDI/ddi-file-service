package uk.ac.ebi.ddi.ddifileservice.configuration;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({ S3Properties.class, FileProperties.class })
public class FileConfiguration {
}
