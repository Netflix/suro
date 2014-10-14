package com.netflix.suro.sink.remotefile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.netflix.suro.sink.localfile.FileNameFormatter;
import com.netflix.suro.sink.localfile.LocalFileSink;
import com.netflix.suro.sink.notice.Notice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jettison.json.JSONObject;

import java.util.Properties;

public class HdfsFileSink extends RemoteFileSink {
    public static final String TYPE = "hdfs";

    private final String directory;
    private final Notice<String> notice;
    private final Configuration hadoopConf;

    @JsonCreator
    public HdfsFileSink(
            @JsonProperty("localFileSink") LocalFileSink localFileSink,
            @JsonProperty("directory") String directory,
            @JsonProperty("concurrentUpload") int concurrentUpload,
            @JsonProperty("notice") Notice notice,
            @JsonProperty("prefixFormatter") RemotePrefixFormatter prefixFormatter,
            @JsonProperty("batchUpload") boolean batchUpload,
            @JsonProperty("properties") Properties properties
    ) {
        super(localFileSink, prefixFormatter, concurrentUpload, batchUpload);

        this.directory = directory;
        this.notice = notice;
        hadoopConf = new Configuration();
        if (properties != null) {
            for (String propertyName : properties.stringPropertyNames()) {
                hadoopConf.set(propertyName, properties.getProperty(propertyName));
            }
        }

        Preconditions.checkNotNull(directory, "directory is needed");
    }

    @Override
    public String recvNotice() {
        return notice.recv();
    }

    @Override
    public long checkPause() {
        return localFileSink.checkPause();
    }

    @Override
    void initialize() {
        // do nothing
    }

    @Override
    void upload(String localFilePath, String remoteFilePath) throws Exception {
        Path outFile = new Path(String.format("%s/%s", directory, remoteFilePath));
        FileSystem fs = outFile.getFileSystem(hadoopConf);

        fs.mkdirs(outFile.getParent());
        fs.moveFromLocalFile(new Path(localFilePath), outFile);
    }

    @Override
    void notify(String filePath, long fileSize) throws Exception {
        JSONObject jsonMessage = new JSONObject();
        jsonMessage.put("directory", directory);
        jsonMessage.put("filePath", filePath);
        jsonMessage.put("size", fileSize);
        jsonMessage.put("collector", FileNameFormatter.localHostAddr);

        if (!notice.send(jsonMessage.toString())) {
            throw new RuntimeException("Notice failed");
        }
    }
}
