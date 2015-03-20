package anagramfrequency;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
/**
 *
 * @author Bhavana Senapathi
 *
 */
public class AnagramMapper extends MapReduceBase implements
                Mapper<LongWritable, Text, Text, IntWritable> {

	    private final IntWritable one = new IntWritable(1);
		private Text sortedText = new Text();

        public void map(LongWritable key, Text value,
                        OutputCollector<Text, IntWritable> outputCollector, Reporter reporter)
                        throws IOException {

        	    StringTokenizer words = new StringTokenizer(value.toString(),
        	          " \t\n\r\f,.:()!?", false);
        	    while (words.hasMoreTokens()) {
        	    	String originalText = words.nextToken().trim().toLowerCase();
        	    	char[] wordChars = originalText.toCharArray();
	                Arrays.sort(wordChars);
	                String sortedWord = new String(wordChars);
	                sortedText.set(sortedWord);
	                outputCollector.collect(sortedText, one);
        	    }
        }

}

