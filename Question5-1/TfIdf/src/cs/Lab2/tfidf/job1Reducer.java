package cs.Lab2.tfidf;

import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class job1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
 
// Reducer1 input : ( word@doc , 1 )
    public job1Reducer() {
    }
 
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    		
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        //write in the context
        context.write(key, new IntWritable(sum));
    }
}
//Reducer1 output : (word@doc , wordCount)