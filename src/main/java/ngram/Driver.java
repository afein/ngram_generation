package ngram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class Driver{
	public static void main(String[] args) throws Exception {
		JobConf job = new JobConf(new Configuration(), Driver.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

        job.setJobName("ngram-generation");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setJarByClass(NgramMapper.class);
        job.setJarByClass(NgramReducer.class);
        job.setMapperClass(NgramMapper.class);
        job.setReducerClass(NgramReducer.class);

        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setNumReduceTasks(8);

		JobClient.runJob(job);
	}
}
