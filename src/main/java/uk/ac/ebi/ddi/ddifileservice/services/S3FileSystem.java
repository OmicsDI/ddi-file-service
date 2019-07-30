package uk.ac.ebi.ddi.ddifileservice.services;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import uk.ac.ebi.ddi.ddifileservice.configuration.FileProperties;
import uk.ac.ebi.ddi.ddifileservice.configuration.S3Properties;
import uk.ac.ebi.ddi.ddifileservice.type.ConvertibleOutputStream;
import uk.ac.ebi.ddi.ddifileservice.utils.FilenameUtils;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(S3FileSystem.class);

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
        try {
            S3Object s3Object = s3Client.getObject(s3Properties.getBucketName(), filePath);
            return s3Object.getObjectContent();
        } catch (AmazonS3Exception e) {
            LOGGER.error("Unable to get file {}", filePath);
            throw e;
        }
    }

    @Override
    public File getFile(String filePath) throws IOException {
        String extension = FilenameUtils.getFileExtension(filePath);
        File file = File.createTempFile("omics-tmp-file", "." + extension);
        try (InputStream in = getInputStream(filePath)) {
            Files.copy(in, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
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
        } catch (AmazonS3Exception e) {
            LOGGER.error("Unable to save file {}", filePath);
            throw e;
        }
    }

    @Override
    public void copyFile(File localFile, String destinationFile) {
        try {
            s3Client.putObject(s3Properties.getBucketName(), destinationFile, localFile);
        } catch (AmazonS3Exception e) {
            LOGGER.error("Unable to upload file {}", destinationFile);
            throw e;
        }
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
