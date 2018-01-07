// (c) Copyright 2009 Cloudera, Inc.
// Hadoop 0.20.1 API Updated by Marcello de Sales (marcello.desales@gmail.com)
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
 
public class job1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Text filename = new Text();
	
    public job1Mapper() {
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    		//Name of the document
    		String filenameString = ((FileSplit) context.getInputSplit()).getPath().getName();
    		filename.set(filenameString);
    		
    		//Decompose the line into words and erase special caracters
        for (String token: value.toString().replaceAll("[^a-zA-Z ]", " ")
          		 .split("\\s+")) {
        	word.set(token);
        	
        	// Create the new key --> word@doc 
        	StringBuilder valueBuilder = new StringBuilder();
        	valueBuilder.append(word);
        	valueBuilder.append("@");
        	valueBuilder.append(filename);
        	//Write the key and the value (1) into the context
        context.write(new Text(valueBuilder.toString()), one);
           }
        }
    }
// Mapper1 output : word@doc \t wordCount