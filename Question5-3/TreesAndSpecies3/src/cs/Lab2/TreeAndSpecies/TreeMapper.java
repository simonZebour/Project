package cs.Lab2.TreeAndSpecies;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


//To complete according to your problem
public class TreeMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	
	private static boolean FALSE = false;
	private static String HAUTEUR = "HAUTEUR";

//We map with species as a key and height as a value
@Override
protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
 {
     // To complete according to the processing
     // Processing : keyI = ..., valI = ...
		String line = valE.toString();
		String[] tab = line.split(";");
		
		Text species = new Text(tab[3]);
		if(tab[6].isEmpty() == FALSE && tab[6].equals(HAUTEUR) == FALSE) {
		FloatWritable height = new FloatWritable(Float.parseFloat(tab[6]));
		context.write(species, height);
		}
		else {System.out.print(tab[3]);
		}
 }

public void run(Context context) throws IOException, InterruptedException {
 setup(context);
 while (context.nextKeyValue()) {
     map(context.getCurrentKey(), context.getCurrentValue(), context);
 }
 cleanup(context);
}
}
