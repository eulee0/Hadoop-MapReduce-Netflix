package hadoop.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UserRatingDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new UserRatingDriver(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			return -1;
		}
		Job job = new Job();
		job.setJarByClass(UserRatingDriver.class);
		job.setJobName("UserRating");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(UserRatingMapper.class);
		job.setReducerClass(UserRatingReducer.class);
		int returnValue = job.waitForCompletion(true) ? 0 : 1;
		if (job.isSuccessful()) {
			System.out.println("Job was successful");
		} else if (!job.isSuccessful()) {
			System.out.println("Job was not successful");
		}
		return returnValue;
	}
}