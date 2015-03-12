package ngram;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class NgramReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable sum = new IntWritable();

	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		int count = 0;
		while (values.hasNext()) {
			count += values.next().get();
			reporter.progress();
		}

		sum.set(count);
		output.collect(key, sum);
	}
}