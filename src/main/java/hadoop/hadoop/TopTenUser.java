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

public class TopTenUser {
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.exit(0);
		}
		String inputFile = args[0];
		String outputFile = args[1];
		HashMap<String, Integer> userMap = getUsers(inputFile);
		@SuppressWarnings("unchecked")
		Map<String, Integer> sort = sortByValues(userMap);
		toFile(outputFile, sort);
	}

	private static HashMap<String, Integer> getUsers(String file) throws NumberFormatException, IOException {
		HashMap<String, Integer> userMap = new HashMap<String, Integer>();
		BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
		String line = "";
		while ((line = bufferedReader.readLine()) != null) {
			StringTokenizer stringTokenizer = new StringTokenizer(line, ",");
			String id = stringTokenizer.nextToken().trim();
			String number = stringTokenizer.nextToken().trim();
			int ratings = Integer.parseInt(number);
			userMap.put(id, ratings);
		}
		bufferedReader.close();
		return userMap;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static HashMap sortByValues(HashMap hashMap) {
		List<String> list = new LinkedList(hashMap.entrySet());
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

	private static void toFile(String file, Map<String, Integer> map) throws IOException {
		List<String> list = new ArrayList<String>(map.keySet());
		PrintWriter printWriter = new PrintWriter(new BufferedWriter(new FileWriter(file)));
		for (int i = 0; i < 10; i++) {
			printWriter.println(list.get(i) + " " + map.get(list.get(i)));
		}
		printWriter.close();
	}
}