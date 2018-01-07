package cs.Lab2.tfidf;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class job3Mapper extends Mapper<LongWritable, Text, Text, Text> {

    public job3Mapper() {
    }
// Mapper input : word@docId /t wordCount/wordsPerDoc

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordDocAndWordcountWordperdoc = value.toString().split("\t");
        String[] wordAndDoc = wordDocAndWordcountWordperdoc[0].split("@");
        Text word = new Text(wordAndDoc[0]);
        String doc = wordAndDoc[1];
        String WordcountWordperdoc = wordDocAndWordcountWordperdoc[1];
        
        context.write(word, new Text(doc + "=" + WordcountWordperdoc));
    }
}
// Mapper output : word /t docId=WordCount/WordPerDoc
