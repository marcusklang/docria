package se.lth.cs.docria.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import se.lth.cs.docria.io.DocumentStreamWriter;

import java.io.IOException;

public class DocriaWriter extends RecordWriter<DocumentWritable, NullWritable> {

    private DocumentStreamWriter writer;

    public DocriaWriter(DocumentStreamWriter writer) {
        this.writer = writer;
    }

    @Override
    public void write(DocumentWritable key, NullWritable value) throws IOException, InterruptedException {
        byte[] msgpackBlob = key.getMsgpackBlob();

        synchronized (this) {
            writer.write(msgpackBlob);
        }
    }

    @Override
    public synchronized void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if(writer == null)
            return;

        writer.close();
        writer = null;
    }
}
