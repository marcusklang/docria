package se.lth.cs.docria.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import se.lth.cs.docria.io.DocumentStreamBoundaryWriter;

import java.io.IOException;

public class DocriaOutputFormat extends FileOutputFormat<DocumentWritable, NullWritable> {

    @Override
    public RecordWriter<DocumentWritable, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration conf = taskAttemptContext.getConfiguration();

        int numDocsPerBlock = conf.getInt("docria.block.size", 128);

        Path file = getDefaultWorkFile(taskAttemptContext, ".docria");
        FileSystem fs = file.getFileSystem(conf);
        return new DocriaWriter(new DocumentStreamBoundaryWriter(fs.create(file), numDocsPerBlock));
    }
}
