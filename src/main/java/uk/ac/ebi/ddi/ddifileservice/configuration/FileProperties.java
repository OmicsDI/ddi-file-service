package uk.ac.ebi.ddi.ddifileservice.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("file")
public class FileProperties {
    private String provider;

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    @Override
    public String toString() {
        return "FileProperties{" +
                "provider='" + provider + '\'' +
                '}';
    }
}
