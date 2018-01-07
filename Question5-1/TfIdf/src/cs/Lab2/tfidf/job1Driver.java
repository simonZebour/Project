package cs.Lab2.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// for each word and doc, job1 calculate the number of word in the doc #WordCount
public class job1Driver extends Configured implements Tool {

 // output 
 private static final String  OUTPUT = "outputJob1";

 // Input 
 private static final String INPUT = "input";

 public int run(String[] args) throws Exception {

     Configuration conf = getConf();
     Job job = new Job(conf, "Job1");

     job.setJarByClass(job1Driver.class);
     job.setMapperClass(job1Mapper.class);
     job.setReducerClass(job1Reducer.class);
     job.setCombinerClass(job1Reducer.class);

     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(IntWritable.class);

     FileInputFormat.addInputPath(job, new Path(INPUT));
     FileOutputFormat.setOutputPath(job, new Path(OUTPUT));

     return job.waitForCompletion(true) ? 0 : 1;
 }

 public static void main(String[] args) throws Exception {
     int res = ToolRunner.run(new Configuration(), new job1Driver(), args);
     System.exit(res);
 }
}
