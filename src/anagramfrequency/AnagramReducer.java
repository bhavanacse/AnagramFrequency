package anagramfrequency;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author Bhavana Senapathi
 *
 */

public class AnagramReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
       
        public void reduce(Text anagramKey, Iterator<IntWritable> anagramValues,
                        OutputCollector<Text, IntWritable> results, Reporter reporter) throws IOException {
        	int sum = 0;
            while (anagramValues.hasNext()) {
              IntWritable value = (IntWritable) anagramValues.next();
              sum += value.get(); // process value
            }

            results.collect(anagramKey, new IntWritable(sum));
        }

}
