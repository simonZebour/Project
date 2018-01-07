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
public class TreeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private static boolean FALSE = false;
	private static String HAUTEUR = "HAUTEUR";

//Overriding of the map method
@Override
protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
 {
     // To complete according to the processing
     // Processing : keyI = ..., valI = ...
		String line = valE.toString();
		String[] tab = line.split(";");
		
		Text species = new Text(tab[3]);
		//Erase header line and empty species
		if(tab[3].isEmpty() == FALSE && tab[6].equals(HAUTEUR) == FALSE) {
		context.write(species, one);
		}
		else{System.out.print(tab[3]);}
 }

public void run(Context context) throws IOException, InterruptedException {
 setup(context);
 while (context.nextKeyValue()) {
     map(context.getCurrentKey(), context.getCurrentValue(), context);
 }
 cleanup(context);
}
}
