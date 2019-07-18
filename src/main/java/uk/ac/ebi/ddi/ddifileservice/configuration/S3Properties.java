package uk.ac.ebi.ddi.ddifileservice.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("s3")
public class S3Properties {
    private String endpointUrl;

    private String accessKey;

    private String secretKey;

    private String bucketName;

    private String region;

    private boolean envAuth = false;

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getEndpointUrl() {
        return endpointUrl;
    }

    public void setEndpointUrl(String endpointUrl) {
        this.endpointUrl = endpointUrl;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public boolean isEnvAuth() {
        return envAuth;
    }

    public void setEnvAuth(boolean envAuth) {
        this.envAuth = envAuth;
    }

    @Override
    public String toString() {
        return "S3Properties{" +
                "endpointUrl='" + endpointUrl + '\'' +
                ", accessKey='" + accessKey + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", bucketName='" + bucketName + '\'' +
                '}';
    }
}
