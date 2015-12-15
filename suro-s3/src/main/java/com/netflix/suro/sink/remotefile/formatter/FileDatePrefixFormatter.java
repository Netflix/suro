package com.netflix.suro.sink.remotefile.formatter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.suro.sink.localfile.FileNameFormatter;
import com.netflix.suro.sink.remotefile.RemotePrefixFormatter;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;

/**
 * Created by liuzhenchuan@foxmail.com on 10/6/14.
 */
public class FileDatePrefixFormatter implements RemotePrefixFormatter{
    public static final String TYPE = "FileDate";
    private final DateTimeFormatter formatter;
    //lastModify or dateCreated
    private String dateType;

    public FileDatePrefixFormatter(@JsonProperty("format") String formatString,
                                   @JsonProperty("dateType") String dateType){
        formatter = DateTimeFormat.forPattern(formatString);
        this.dateType = dateType;

    }

    @Override
    public String get(File file) {
        String prefix = formatter.print(getDateCreated(file));//use dateCreated as default.
        if("lastModify".equalsIgnoreCase(dateType)){
            prefix = formatter.print(file.lastModified());
        }
        if(!prefix.endsWith("/")) prefix += "/";
        return prefix;
    }

    private DateTime getDateCreated(File file){
        return FileNameFormatter.getFileCreateTime(file);
    }

}
