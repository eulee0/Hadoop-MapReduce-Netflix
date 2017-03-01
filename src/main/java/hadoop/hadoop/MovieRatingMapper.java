package hadoop.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieRatingMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text text = new Text();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, ",");
		String movieId = st.nextToken();
		text.set(movieId);
		String rating = "";
		st.nextToken();
		rating = st.nextToken();
		context.write(text, new Text(rating));
	}
}