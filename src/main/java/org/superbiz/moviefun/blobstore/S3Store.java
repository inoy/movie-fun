package org.superbiz.moviefun.blobstore;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

public class S3Store implements BlobStore {

    Logger logger = Logger.getLogger(S3Store.class.getName());

    private AmazonS3Client s3Client;
    private String bucketName;

    public S3Store(AmazonS3Client s3Client, String bucketName) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
    }

    @Override
    public void put(Blob blob) throws IOException {
        s3Client.putObject(bucketName, blob.name, blob.inputStream, new ObjectMetadata());
    }

    @Override
    public Optional<Blob> get(String name) throws IOException {
        S3Object s3obj = s3Client.getObject(bucketName, name);
        Blob blob = new Blob(name, s3obj.getObjectContent(), MediaType.IMAGE_PNG_VALUE);
        return Optional.ofNullable(blob);
    }

    @Override
    public void deleteAll() {
        for (S3ObjectSummary objectSummary : s3Client.listObjects(bucketName).getObjectSummaries()) {
            try {
                s3Client.deleteObject(bucketName, objectSummary.getKey());
            } catch (Exception e) {
                logger.warning(e.getMessage());
            }
        }
    }

}
