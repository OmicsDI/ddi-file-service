package uk.ac.ebi.ddi.ddis3service.services;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import uk.ac.ebi.ddi.ddis3service.configuration.S3Properties;
import uk.ac.ebi.ddi.ddis3service.type.ConvertibleOutputStream;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Service
public class AmazonS3Service {

    private AmazonS3 s3Client;

    @Autowired
    private S3Properties s3Properties;

    @PostConstruct
    private void initializeAmazon() {
        if (!s3Properties.isLocal()) {
            AWSCredentials credentials = new BasicAWSCredentials(
                    s3Properties.getAccessKey(), s3Properties.getSecretKey());
            s3Client = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .build();
        } else {
            s3Client = AmazonS3ClientBuilder.standard()
                    .build();
        }
    }

    public boolean isBucketExists(String bucketName) {
        return s3Client.doesBucketExistV2(bucketName);
    }

    public InputStream getObject(String bucketName, String filePath) {
        S3Object s3Object = s3Client.getObject(bucketName, filePath);
        return s3Object.getObjectContent();
    }

    public void uploadObject(String bucketName, String filePath, ConvertibleOutputStream outputStream)
            throws IOException {
        try (InputStream inputStream = outputStream.toInputStream()) {
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(outputStream.getSize());
            s3Client.putObject(bucketName, filePath, inputStream, meta);
        }
    }

    public void uploadFile(String bucketName, String filePath, File file) {
        s3Client.putObject(bucketName, filePath, file);
    }

    public List<S3ObjectSummary> getObjectsFromFolder(String bucketName, String folderKey) {

        ListObjectsRequest listObjectsRequest =
                new ListObjectsRequest()
                        .withBucketName(bucketName)
                        .withPrefix(folderKey + "/");

        List<S3ObjectSummary> keys = new ArrayList<>();

        ObjectListing objects = s3Client.listObjects(listObjectsRequest);
        for (;;) {
            List<S3ObjectSummary> summaries = objects.getObjectSummaries();
            if (summaries.size() < 1) {
                break;
            }
            keys.addAll(summaries);
            objects = s3Client.listNextBatchOfObjects(objects);
        }

        return keys;
    }
}
