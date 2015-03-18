package mapred.ngramcount;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class NgramCountReducer extends Reducer<Text, NullWritable, Text, IntWritable> {
	private IntWritable sum = new IntWritable();

	public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		int count = 0;

		for (NullWritable n : values)
			count++;

		sum.set(count);
		context.write(key, sum);
	}
}
