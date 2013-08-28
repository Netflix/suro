package com.netflix.suro.sink.localfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SequenceFileViewer {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        SequenceFile.Reader r = new SequenceFile.Reader(fs, new Path(args[0]), conf);
        Text routingKey = new Text();
        SequenceFileWriter.MessageWritable message = new SequenceFileWriter.MessageWritable();

        while (r.next(routingKey, message)) {
            System.out.println("###routing key: " + routingKey);
            System.out.println(message.getSerDe().toString(message.getMessage().getPayload()));
        }

        r.close();
    }
}
