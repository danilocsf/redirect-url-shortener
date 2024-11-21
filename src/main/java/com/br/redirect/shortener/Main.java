package com.br.redirect.shortener;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class Main implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private final static String BUCKET_NAME = "url-shortener-study2-danilo";

    private final S3Client s3Client = S3Client.builder().build();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {
        String pathParameters = (String)input.get("rawPath");
        String shortUrlCode = pathParameters.replace("/", "");
        if(shortUrlCode == null || shortUrlCode.isEmpty()){
            throw new IllegalArgumentException("Invalid input: shortUrlCode is required");
        }
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(shortUrlCode + ".json")
                .build();

        InputStream s3ObjectStream;
        try {
            s3ObjectStream = s3Client.getObject(getObjectRequest);
        } catch(Exception e) {
            throw new RuntimeException("Error fetching URL data from S3:" + e.getMessage());
        }
        UrlData urlData;
        try {
            urlData = objectMapper.readValue(s3ObjectStream, UrlData.class);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing URL Data:"+ e.getMessage(), e);
        }
        long currentTimeInSeconds = System.currentTimeMillis() / 1000;
        Map<String, Object> response = new HashMap<>();
        if (currentTimeInSeconds <= urlData.getExpirationTime()) {
            Map<String, String> headers = new HashMap<>();
            headers.put("Location", urlData.getOriginalUrl());
            response.put("statusCode", 302);
            response.put("headers", headers);
        } else {
            response.put("statusCode", 410);
            response.put("body", "URL has expired");
        }
        return response;
    }
}