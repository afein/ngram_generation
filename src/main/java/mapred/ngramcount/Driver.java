package mapred.ngramcount;

import java.io.IOException;
import mapred.job.Optimizedjob;
import mapred.util.SimpleParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class Driver {

	public static void main(String args[]) throws Exception {
		SimpleParser parser = new SimpleParser(args);

		String input = parser.get("input");
		String output = parser.get("output");
		String n = parser.get("n");

		getJobFeatureVector(input, output, n);

	}

	private static void getJobFeatureVector(String input, String output, String n)
			throws IOException, ClassNotFoundException, InterruptedException {
		Optimizedjob job = new Optimizedjob(new Configuration(), input, output, n,
				"Compute NGram Count");

		job.setClasses(NgramCountMapper.class, NgramCountReducer.class, null);
		job.setMapOutputClasses(Text.class, NullWritable.class);

		job.run();
	}	
}
