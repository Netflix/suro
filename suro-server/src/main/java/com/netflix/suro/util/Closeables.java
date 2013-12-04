/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.suro.util;

import java.io.Closeable;
import java.io.IOException;

import static com.google.common.io.Closeables.close;

/**
 * Utility class that handles {@link java.io.Closeable} objects
 */
public class Closeables {

    public static void closeQuietly(Closeable closeable) {
        try{
            close(closeable, true);
        }catch(IOException e){
            // No need to do anything here as any IOException should
            // have been suppressed here.
        }
    }
}
