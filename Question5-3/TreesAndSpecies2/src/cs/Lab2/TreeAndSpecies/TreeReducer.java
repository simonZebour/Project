package cs.Lab2.TreeAndSpecies;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class TreeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable totalTree = new IntWritable();

    @Override
    public void reduce(final Text _key, final Iterable<IntWritable> values,
            final Context context) throws IOException, InterruptedException {

        int sum = 0;
        Iterator<IntWritable> iterator = values.iterator();

        while (iterator.hasNext()) {
            sum += iterator.next().get();
        }

        totalTree.set(sum);
        // context.write(key, new IntWritable(sum));
        context.write(_key, totalTree);
    }
}
