package cs.Lab2.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class job2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    public job2Mapper() {
    }
    
//Mapper2 input : (word@doc , wordCount)
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordDocAndCounter = value.toString().split("\t");
        String[] wordAndDoc = wordDocAndCounter[0].split("@");
        Text doc = new Text(wordAndDoc[1]);
        Text wordWordcount = new Text(wordAndDoc[0] + "=" + wordDocAndCounter[1]);

        context.write(doc, wordWordcount);
    }
}
//Mapper2 output : (doc , word = wordCount)