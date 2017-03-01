package hadoop.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserRatingMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer stringTokenizer = new StringTokenizer(line, ",");
		stringTokenizer.nextToken();
		word.set(stringTokenizer.nextToken());
		context.write(word, one);

	}
}