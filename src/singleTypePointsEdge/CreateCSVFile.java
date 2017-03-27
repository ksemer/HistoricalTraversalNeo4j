package singleTypePointsEdge;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CreateCSVFile {

	private static final String DATASET_PATH = "data/dblp_merge";
	private static final String authors_csv = "/experiments/neo4j-community-3.1.1/import/nodes.csv";
	private static final String published_csv = "/experiments/neo4j-community-3.1.1/import/connected.csv";
	private static Map<Integer, String> nodesNames;
	private static Map<Integer, Set<Integer>> nodes = new HashMap<>();

	public static void main(String[] args) throws IOException {
		loadDataset();
	}

	private static void loadDataset() throws IOException {

		BufferedReader br = new BufferedReader(new FileReader(DATASET_PATH));
		String line = null;
		String[] edge, time;
		Set<Integer> n1_t, n2_t;

		// Set<Integer>
		long executionTime = System.currentTimeMillis();

		while ((line = br.readLine()) != null) {
			edge = line.split("\t");

			n1_t = findCreateAuthor(Integer.parseInt(edge[0]));
			n2_t = findCreateAuthor(Integer.parseInt(edge[1]));

			// edge[2] has the year/time
			time = edge[2].split(" ");

			for (String inter : time) {
				String[] in = inter.split(",");

				for (int i = Integer.parseInt(in[0]); i <= Integer.parseInt(in[1]); i++) {
					n1_t.add(i);
					n2_t.add(i);
				}
			}
		}
		br.close();

		createNodesAndAdjacencies();

		System.out.println("Loadtime of all: " + (System.currentTimeMillis() - executionTime) + " (ms)");
	}

	private static void createNodesAndAdjacencies() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(DATASET_PATH));
		FileWriter w = new FileWriter(published_csv);
		FileWriter acs = new FileWriter(authors_csv);

		String line = null;
		String[] edge;

		if (DATASET_PATH.contains("dblp")) {
			loadNames();
			acs.write("id:ID(Node-ID),name,time_instances:int[],:LABEL\n");
		} else
			acs.write("id:ID(Node-ID),time_instances:int[],:LABEL\n");

		w.write(":START_ID(Node-ID),:END_ID(Node-ID),time_instances:int[],:TYPE\n");

		while ((line = br.readLine()) != null) {
			edge = line.split("\t");
			String[] interval = edge[2].split(" ");

			List<Integer> times = new ArrayList<>();

			for (String inter : interval) {
				String[] in = inter.split(",");

				for (int t = Integer.parseInt(in[0]); t <= Integer.parseInt(in[1]); t++) {
					times.add(t);
				}
			}

			String time_instances = "";

			for (int i = 0; i < times.size(); i++)
				time_instances += times.get(i) + ";";

			time_instances = time_instances.substring(0, time_instances.length() - 1);

			if (DATASET_PATH.contains("dblp"))
				w.write(edge[0] + "," + edge[1] + "," + time_instances + ",PUBLISH\n");
			else
				w.write(edge[0] + "," + edge[1] + "," + time_instances + ",ISFRIEND\n");
		}
		br.close();

		for (int a : nodes.keySet()) {
			int[] times = new int[nodes.get(a).size()];
			int i = 0;

			for (int t : nodes.get(a)) {
				times[i++] = t;
			}

			Arrays.sort(times);
			String time_instances = "";

			for (i = 0; i < times.length; i++) {
				time_instances += times[i] + ";";
			}
			time_instances = time_instances.substring(0, time_instances.length() - 1);

			if (DATASET_PATH.contains("dblp"))
				acs.write(a + ",\"" + nodesNames.get(a) + "\"," + time_instances + ",Author\n");
			else
				acs.write(a + "," + time_instances + ",User\n");

		}
		w.close();
		acs.close();
	}

	private static Set<Integer> findCreateAuthor(int id) throws IOException {
		Set<Integer> n;

		if ((n = nodes.get(id)) == null) {
			n = new HashSet<>();
			nodes.put(id, n);
		}

		return n;
	}

	private static void loadNames() throws IOException {
		System.out.println("Loading authors names in memory...");

		BufferedReader br = new BufferedReader(new FileReader(DATASET_PATH + "_authors_ids"));
		String line = null;
		String[] token;

		nodesNames = new HashMap<>();

		while ((line = br.readLine()) != null) {
			token = line.split("\t");
			token[1] = token[1].replaceAll("\"", "");
			nodesNames.put(Integer.parseInt(token[0]), token[1]);
		}
		br.close();
	}
}