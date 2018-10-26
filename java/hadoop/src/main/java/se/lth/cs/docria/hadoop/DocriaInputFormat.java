package se.lth.cs.docria.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import se.lth.cs.docria.io.BoundaryReader;
import se.lth.cs.docria.io.DocumentStreamReader;

import java.io.IOException;

public class DocriaInputFormat extends FileInputFormat<DocumentWritable, NullWritable> {
    @Override
    public RecordReader<DocumentWritable, NullWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new RecordReader<DocumentWritable, NullWritable>() {
            private DocumentStreamReader streamReader;
            private DocumentWritable current = new DocumentWritable();
            private Configuration conf;
            private long numBytes = 0L;
            private FSDataInputStream fsInput;

            @Override
            public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException,
                    InterruptedException {
                FileSplit fileSplit = (FileSplit)inputSplit;
                numBytes = fileSplit.getLength();
                conf = taskAttemptContext.getConfiguration();
                Path path = fileSplit.getPath();
                FileSystem fs = path.getFileSystem(conf);
                fsInput = fs.open(path);
                streamReader = new DocumentStreamReader(fsInput);
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                if(streamReader == null)
                    return false;

                byte[] rawData = streamReader.nextRaw();
                if(rawData == null) {
                    streamReader.close();
                    fsInput = null;
                    streamReader = null;
                    return false;
                }

                current.setMsgpackBlob(rawData);
                return true;
            }

            @Override
            public DocumentWritable getCurrentKey() throws IOException, InterruptedException {
                return current;
            }

            @Override
            public NullWritable getCurrentValue() throws IOException, InterruptedException {
                return NullWritable.get();
            }

            @Override
            public float getProgress() throws IOException, InterruptedException {
                if(fsInput == null && numBytes > 0)
                    return 1.0f;
                else if(fsInput == null)
                    return 0.0f;

                return fsInput.getPos() / (float)numBytes;
            }

            @Override
            public void close() throws IOException {
                if(streamReader != null) {
                    streamReader.close();
                    fsInput = null;
                    streamReader = null;
                }
            }
        };
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
