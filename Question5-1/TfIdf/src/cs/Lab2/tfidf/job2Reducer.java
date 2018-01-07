package cs.Lab2.tfidf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class job2Reducer extends Reducer<Text, Text, Text, Text> { 
    public job2Reducer() {
    }
//Reducer2 input : (doc , word = wordCount)
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int wordsperdoc = 0;
        // Create a Tab to save the different words and wordcount associated
        Map<String, Integer> wordAndWordcountTab = new HashMap<String, Integer>();
        //We iterate over the different words in the doc
        for (Text val : values) {
            String[] wordAndWordcount = val.toString().split("=");
            String word = wordAndWordcount[0];
            String Wordcount = wordAndWordcount[1];
            wordAndWordcountTab.put(word, Integer.valueOf(Wordcount));
            //We increament the number of words by the wordcount of the word in that document
            wordsperdoc += Integer.parseInt(Wordcount);
        }
        for (String word : wordAndWordcountTab.keySet()) {
        		//for each word and each document (key), we write the new result in the context
            context.write(new Text(word + "@" + key.toString()), new Text(wordAndWordcountTab.get(word) + "/"
                    + wordsperdoc));
        }
    }
}
//Reducer 2 output : (word@docId, wordCount/wordsPerDoc)
