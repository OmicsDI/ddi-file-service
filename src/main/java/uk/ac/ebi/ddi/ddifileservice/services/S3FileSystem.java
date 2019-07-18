package uk.ac.ebi.ddi.ddifileservice.services;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import uk.ac.ebi.ddi.ddifileservice.configuration.FileProperties;
import uk.ac.ebi.ddi.ddifileservice.configuration.S3Properties;
import uk.ac.ebi.ddi.ddifileservice.type.ConvertibleOutputStream;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

@Component
@ConditionalOnProperty(
        value = "file.provider",
        havingValue = "s3",
        matchIfMissing = true
)
public class S3FileSystem implements IFileSystem {

    private AmazonS3 s3Client;

    @Autowired
    private S3Properties s3Properties;

    @Autowired
    private FileProperties fileProperties;

    @PostConstruct
    private void initializeAmazon() {
        if (!fileProperties.getProvider().equals("s3")) {
            return;
        }
        if (!s3Properties.isEnvAuth()) {
            AWSCredentials credentials = new BasicAWSCredentials(
                    s3Properties.getAccessKey(), s3Properties.getSecretKey());
            s3Client = AmazonS3ClientBuilder
                    .standard()
                    .withEndpointConfiguration(new AwsClientBuilder
                            .EndpointConfiguration(s3Properties.getEndpointUrl(), s3Properties.getRegion()))
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withPathStyleAccessEnabled(true)
                    .build();
        } else {
            s3Client = AmazonS3ClientBuilder.standard()
                    .withEndpointConfiguration(new AwsClientBuilder
                            .EndpointConfiguration(s3Properties.getEndpointUrl(), s3Properties.getRegion()))
                    .withPathStyleAccessEnabled(true)
                    .build();
        }

        if (!s3Client.doesBucketExistV2(s3Properties.getBucketName())) {
            throw new RuntimeException("S3 bucket doesn't exists: " + s3Properties.getBucketName());
        }
    }

    @Override
    public InputStream getInputStream(String filePath) {
        S3Object s3Object = s3Client.getObject(s3Properties.getBucketName(), filePath);
        return s3Object.getObjectContent();
    }

    @Override
    public File getFile(String filePath) throws IOException {
        File file = File.createTempFile("omics-tmp-file", ".tmp");
        try (InputStream in = getInputStream(filePath)) {
            Files.copy(in, file.toPath());
        }
        file.deleteOnExit();
        return file;
    }

    @Override
    public void saveFile(ConvertibleOutputStream outputStream, String filePath) throws IOException {
        try (InputStream inputStream = outputStream.toInputStream()) {
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(outputStream.getSize());
            s3Client.putObject(s3Properties.getBucketName(), filePath, inputStream, meta);
        }
    }

    @Override
    public void copyFile(File localFile, String destinationFile) {
        s3Client.putObject(s3Properties.getBucketName(), destinationFile, localFile);
    }

    @Override
    public List<String> listFilesFromFolder(String folderPath) {
        ListObjectsRequest listObjectsRequest =
                new ListObjectsRequest()
                        .withBucketName(s3Properties.getBucketName())
                        .withPrefix(folderPath + "/");

        List<String> keys = new ArrayList<>();

        ObjectListing objects = s3Client.listObjects(listObjectsRequest);
        for (;;) {
            List<S3ObjectSummary> summaries = objects.getObjectSummaries();
            if (summaries.size() < 1) {
                break;
            }
            summaries.forEach(x -> keys.add(x.getKey()));
            objects = s3Client.listNextBatchOfObjects(objects);
        }

        return keys;
    }

    @Override
    public void deleteFile(String filePath) {
        s3Client.deleteObject(s3Properties.getBucketName(), filePath);
    }

    @Override
    public boolean isFile(String filePath) {
        return s3Client.doesObjectExist(s3Properties.getBucketName(), filePath);
    }

    @Override
    public void cleanDirectory(String dirPath) {
        List<String> files = listFilesFromFolder(dirPath);
        for (String file : files) {
            deleteFile(file);
        }
    }
}
