package ngram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class NgramMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	private Text phrase = new Text();
	private IntWritable one = new IntWritable(1);


	@Override
	public void map(LongWritable longWritable, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		String line = value.toString();
		if (!line.isEmpty()) {
            line = line.replaceAll("[^a-zA-Z]", " ").toLowerCase();	// only consider ngrams per line
			String[] tokens = line.trim().split(" +");

            for (int j = 0; j < tokens.length; j++) {
				String curPhrase = tokens[j];
				if (curPhrase.equals("") || curPhrase.equals(" ")) {
					continue;
				}
				phrase.set(curPhrase);
				output.collect(phrase, one);
                for (int i = 1; i<5; i++) { 	// 2-5 ngrams
					if (i+j < tokens.length) {
						curPhrase += " " + tokens[i+j];
						phrase.set(curPhrase);
						output.collect(phrase, one);
					}
                }
            }
			reporter.progress();
		}
	}
}