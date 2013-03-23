package com.netflix.suro.sink.localfile;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.server.UID;

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

    public static String get(String dir) {
        StringBuilder sb = new StringBuilder(dir);
        if (dir.endsWith("/") == false) {
            sb.append('/');
        }
        sb.append(fmt.print(new DateTime()))
                .append(localHostAddr)
                .append(new UID().toString());
        return sb.toString().replaceAll("[-:]", "");
    }
}
