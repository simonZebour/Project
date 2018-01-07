package cs.Lab2.TreeAndSpecies;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//We reduce with the max height of each species
public class TreeReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    private FloatWritable maxHeight = new FloatWritable();

    @Override
    public void reduce(final Text _key, final Iterable<FloatWritable> values,
            final Context context) throws IOException, InterruptedException {

        float max = new Float(0);
        Iterator<FloatWritable> iterator = values.iterator();

        while (iterator.hasNext()) {
        float newMax = iterator.next().get();
        	if(newMax > max) {
            max = newMax;
        	}
        }

        maxHeight.set(max);
        // context.write(key, new IntWritable(sum));
        context.write(_key, maxHeight);
    }
}
