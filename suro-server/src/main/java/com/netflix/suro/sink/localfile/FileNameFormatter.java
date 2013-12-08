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

package com.netflix.suro.sink.localfile;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.server.UID;

/**
 * File name formatter used in LocalFileSink
 * File name is formated as [PyyyyMMDDtHHmmss][hostname][random UID]
 *
 * @author jbae
 */
public class FileNameFormatter {
    public static String localHostAddr;
    static {
        try {
            localHostAddr = InetAddress.getLocalHost().getHostName() ;
        } catch (UnknownHostException e) {
            localHostAddr = "UNKONWN_HOST";
        }
    }

    private static DateTimeFormatter fmt = DateTimeFormat.forPattern("'P'yyyyMMdd'T'HHmmss");

    /**
     *
     * @param dir directory path where the files are written
     * @return full file path with the directory suffixed
     */
    public static String get(String dir) {
        StringBuilder sb = new StringBuilder(dir);
        if (!dir.endsWith("/")) {
            sb.append('/');
        }
        sb.append(fmt.print(new DateTime()))
                .append(localHostAddr)
                .append(new UID().toString());
        return sb.toString().replaceAll("[-:]", "");
    }
}
