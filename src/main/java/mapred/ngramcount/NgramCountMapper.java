package mapred.ngramcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import mapred.util.Tokenizer;

public class NgramCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	private Text phrase = new Text();
	private int n;
	private NullWritable nullWritable = NullWritable.get();

	@Override
	public void map(LongWritable longWritable, Text value, Context context) throws IOException, InterruptedException {
		n = Integer.parseInt(context.getConfiguration().get("n"));
		String line = value.toString();
		if (!line.isEmpty()) {
			line = value.toString();
			String[] tokens = Tokenizer.tokenize(line);

			for (int j = 0; j < tokens.length; j++) {
				String curPhrase = tokens[j];
				if (curPhrase.equals("") || curPhrase.equals(" ")) {
					continue;
				}
				phrase.set(curPhrase);
				if (n == 1)
                    context.write(phrase, nullWritable);
				else if (n+j < tokens.length) {
						for (int i = 0; i < n; i++)
                            curPhrase += " " + tokens[i+j];
						phrase.set(curPhrase);
						context.write(phrase, nullWritable);
                }
            }
		}
	}
}
