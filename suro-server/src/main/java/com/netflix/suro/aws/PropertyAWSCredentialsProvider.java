package com.netflix.suro.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.netflix.governator.annotations.Configuration;

/**
 * AWSCredentialsProvider using properties to get accessKey and secretKey.
 * For command line these properties are set by SuroServer
 * 
 * @author metacret
 * @author elandau
 */
public class PropertyAWSCredentialsProvider implements AWSCredentialsProvider {
    @Configuration("SuroServer.AWSAccessKey")
    private String accessKey;
    
    @Configuration("SuroServer.AWSSecretKey")
    private String secretKey;
    
    @Override
    public AWSCredentials getCredentials() {
        if (accessKey != null && secretKey != null) {
            return new AWSCredentials() {
                @Override
                public String getAWSAccessKeyId() {
                    return accessKey;
                }

                @Override
                public String getAWSSecretKey() {
                    return secretKey;
                }
            };
        } 
        else {
            return null;
        }
    }
    
    @Override
    public void refresh() {
        // TODO: Hmmm... what should we do here
    }
}
