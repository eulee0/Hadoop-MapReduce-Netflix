package hadoop.hadoop;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		double movieRating = 0.0;
		int ratings = 0;
		String movie;
		Iterator<Text> iterator = values.iterator();
		while (iterator.hasNext()) {
			movie = iterator.next().toString();
			StringTokenizer tokenizer = new StringTokenizer(movie, ",");
			movieRating += Double.parseDouble(tokenizer.nextToken());
			ratings++;
		}
		String title = "";
		movieRating = movieRating / ratings;
		DecimalFormat df = new DecimalFormat("0.##");
		String movieInfo = "," + title;
		movieInfo += df.format(movieRating);
		Text movieText = new Text(movieInfo);
		context.write(key, movieText);
	}
}
