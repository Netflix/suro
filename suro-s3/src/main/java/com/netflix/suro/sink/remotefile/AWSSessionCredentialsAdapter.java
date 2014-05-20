/*
 * Copyright 2013 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.netflix.suro.sink.remotefile;

import com.amazonaws.auth.AWSCredentialsProvider;
import org.jets3t.service.security.AWSSessionCredentials;

/**
 * AWSCredentialsProvider wrapper for jets3t library
 *
 * @author jbae
 */
public class AWSSessionCredentialsAdapter extends AWSSessionCredentials {
    private final AWSCredentialsProvider provider;

    public AWSSessionCredentialsAdapter(AWSCredentialsProvider provider) {
        super(null, null, null);
        if(provider.getCredentials() instanceof com.amazonaws.auth.AWSSessionCredentials)
            this.provider = provider;
        else
            throw new IllegalArgumentException("provider does not contain session credentials");
    }

    @Override
    protected String getTypeName() {
        return "AWSSessionCredentialsAdapter";
    }

    @Override
    public String getVersionPrefix() {
        return "Netflix AWSSessionCredentialsAdapter, version: ";
    }

    @Override
    public String getAccessKey() {
        return provider.getCredentials().getAWSAccessKeyId();
    }

    @Override
    public String getSecretKey() {
        return provider.getCredentials().getAWSSecretKey();
    }

    public String getSessionToken() {
        com.amazonaws.auth.AWSSessionCredentials sessionCredentials =
                (com.amazonaws.auth.AWSSessionCredentials) provider.getCredentials();
        return sessionCredentials.getSessionToken();
    }
}
