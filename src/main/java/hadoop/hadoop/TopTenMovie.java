package hadoop.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

public class TopTenMovie {
	public static void main(String[] args) throws IOException {
		if (args.length != 3) {
			System.exit(0);
		}
		String ratingsFile = args[0];
		String titlesFile = args[1];
		String outputFile = args[2];
		HashMap<String, Double> ratings = parseMovieRatings(ratingsFile);
		HashMap<String, String> titles = parseTitles(titlesFile);
		Map<String, Double> map = sortByValues(ratings);
		createFile(outputFile, map, titles);

	}

	private static HashMap<String, String> parseTitles(String file) throws IOException {
		HashMap<String, String> titleMap = new HashMap<String, String>();
		BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
		String line = "";
		while ((line = bufferedReader.readLine()) != null) {
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			String id = tokenizer.nextToken().trim();
			String year = tokenizer.nextToken();
			year += " " + tokenizer.nextToken();
			if (tokenizer.hasMoreTokens()) {
				year += "," + tokenizer.nextToken();
			}
			titleMap.put(id, year);
		}
		bufferedReader.close();
		return titleMap;
	}

	private static HashMap<String, Double> parseMovieRatings(String file) throws IOException {
		HashMap<String, Double> ratingMap = new HashMap<String, Double>();
		BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
		String line = "";
		while ((line = bufferedReader.readLine()) != null) {
			StringTokenizer tokenizer = new StringTokenizer(line, ",");
			String id = tokenizer.nextToken().trim();
			String rating = tokenizer.nextToken().trim();
			double ratings = Double.parseDouble(rating);
			ratingMap.put(id, ratings);
		}
		bufferedReader.close();
		return ratingMap;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static HashMap<String, Double> sortByValues(HashMap<String, Double> map) {
		List<String> list = new LinkedList(map.entrySet());
		Collections.sort(list, new Comparator() {
			public int compare(Object o1, Object o2) {
				return ((Comparable) ((Map.Entry) (o2)).getValue()).compareTo(((Map.Entry) (o1)).getValue());
			}
		});
		HashMap sort = new LinkedHashMap();
		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			Map.Entry entry = (Map.Entry) iterator.next();
			sort.put(entry.getKey(), entry.getValue());
		}
		return sort;
	}

	private static void createFile(String output, Map<String, Double> ratings, HashMap<String, String> titles)
			throws IOException {
		List<String> list = new ArrayList<String>(ratings.keySet());
		PrintWriter printWriter = new PrintWriter(new BufferedWriter(new FileWriter(output)));
		for (int i = 0; i < 10; i++) {
			printWriter.println(list.get(i) + ", " + titles.get(list.get(i)) + ", " + ratings.get(list.get(i)));
		}
		printWriter.close();
	}
}