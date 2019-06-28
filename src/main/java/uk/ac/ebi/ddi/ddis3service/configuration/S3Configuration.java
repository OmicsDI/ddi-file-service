package uk.ac.ebi.ddi.ddis3service.configuration;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({ S3Properties.class })
public class S3Configuration {
}
