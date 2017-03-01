package hadoop.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserRatingReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text text, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int total = 0;
		Iterator<IntWritable> iterator = values.iterator();
		while (iterator.hasNext()) {
			int number = iterator.next().get();
			total += number;
		}
		String temp = text.toString() + ",";
		context.write(new Text(temp), new IntWritable(total));
	}
}