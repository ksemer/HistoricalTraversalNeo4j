package utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Create a dblp graph with invtervals u \t v \t year_start,year_end
 * year_start,year_end
 * 
 * @author ksemer
 */
public class DBLP_Merge {
	private static String data = "data/dblp";
	private static String output = "data/dblp_merge";

	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(data));
		FileWriter w = new FileWriter(output);

		String line = null;
		Map<Integer, Map<Integer, Set<Integer>>> g = new HashMap<>();
		Map<Integer, Set<Integer>> map;
		Set<Integer> set;
		String[] token;
		int id1, id2, year;

		while ((line = br.readLine()) != null) {
			token = line.split("\t");

			id1 = Integer.parseInt(token[0]);
			id2 = Integer.parseInt(token[1]);
			year = Integer.parseInt(token[2]);

			if ((map = g.get(id1)) == null) {
				map = new HashMap<>();
				g.put(id1, map);
			}

			if ((set = map.get(id2)) == null) {
				set = new HashSet<>();
				map.put(id2, set);
			}

			set.add(year);
		}
		br.close();

		for (Entry<Integer, Map<Integer, Set<Integer>>> e : g.entrySet()) {

			for (Entry<Integer, Set<Integer>> e1 : e.getValue().entrySet()) {
				w.write(e.getKey() + "\t" + e1.getKey() + "\t" + getIntervals(new ArrayList<Integer>(e1.getValue()))
						+ "\n");
			}
		}
		w.close();
	}

	private static String getIntervals(List<Integer> list) {
		Collections.sort(list);

		int start = list.get(0), end = start;
		String result = "";

		for (int i = 1; i < list.size(); i++) {
			if (list.get(i) == end + 1)
				end++;
			else {
				result += start + "," + end + " ";
				start = list.get(i);
				end = start;
			}
		}

		if (start == end || result.isEmpty() || (!result.isEmpty() && start < end))
			result += start + "," + end;

		return result;
	}
}