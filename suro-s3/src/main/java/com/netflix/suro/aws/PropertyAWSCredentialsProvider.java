package com.netflix.suro.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.netflix.governator.annotations.Configuration;

/**
 * An {@link AWSCredentialsProvider} implementation that is backed by Java properties. It is up to wired
 * {@link com.netflix.governator.configuration.ConfigurationProvider} to set the property values
 * for access key and secret key. If we use {@link com.netflix.suro.SuroServer}, then such properties can
 * be passed in using {@link com.netflix.suro.SuroServer}'s command line parameters. The properties are
 * set at server initialization time, and does not get refreshed.
 *
 * If you want to integrate with the profile-based credential provider, use Amazon's <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/InstanceProfileCredentialsProvider.html">InstanceProfileCredentialsProvider</a>
 *
 * @author jbae
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
    }
}
