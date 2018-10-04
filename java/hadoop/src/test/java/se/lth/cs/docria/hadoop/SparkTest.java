package se.lth.cs.docria.hadoop;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import se.lth.cs.docria.Document;
import se.lth.cs.docria.Layer;
import se.lth.cs.docria.Schema;
import se.lth.cs.docria.Text;
import se.lth.cs.docria.values.Values;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class SparkTest {
    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext("local[*]", "Test");

        List<DocumentWritable> docrias = new ArrayList<>();

        long k = 0;

        for(int i = 0; i < 1024; i++) {
            List<String> somenumbers = LongStream.range(k, k + 128).mapToObj(String::valueOf).collect(Collectors.toList());
            k += 128;

            Document doc = new Document();
            doc.props().put("uri", Values.get(String.format("doc:%d", i)));

            IntArrayList offsets = new IntArrayList(128);
            StringBuilder sb = new StringBuilder();
            for (String somenumber : somenumbers) {
                offsets.add(sb.length());
                sb.append(somenumber);
                offsets.add(sb.length());
                sb.append(" ");
            }

            Text main = doc.add(new Text("main", sb.toString()));

            Layer tokenlayer = doc.add(Schema.layer("token").addField("text", main.spanType()).build());
            for(int j = 0; j < 128; j++) {
                tokenlayer.create().put("text", main.span(offsets.getInt(j*2), offsets.getInt(j*2+1))).insert();
            }

            docrias.add(new DocumentWritable(doc));
        }

        Configuration conf = new Configuration();
        conf.setInt("docria.block.size", 32);



        jsc.parallelize(docrias, 16)
           .mapToPair(d -> new Tuple2<>(d, NullWritable.get()))
           .saveAsNewAPIHadoopFile("file:" + new File("output-test").getAbsolutePath(), DocumentWritable.class, NullWritable.class, DocriaOutputFormat.class, conf);

        int count = jsc.newAPIHadoopFile("file:" + new File("output-test").getAbsolutePath(), DocriaInputFormat.class, DocumentWritable.class, NullWritable.class, new Configuration())
           .map(tup -> tup._1().getDocument().layer("token").size())
           .reduce((x,y) -> x+y);

        System.out.println(count);
        jsc.stop();
    }
}
