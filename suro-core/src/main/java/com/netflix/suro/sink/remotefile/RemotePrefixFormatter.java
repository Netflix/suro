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

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * When uploading files to the remote file system such as S3 or HDFS, the file
 * path should be specified. This interface is describing which file path would
 * be prefixed to the file name.
 *
 * @author jbae
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface RemotePrefixFormatter {
    public String get();
}
