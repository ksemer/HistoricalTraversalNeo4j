package multiTypeEdge;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class CreateCSV {

	private static final String DATASET_PATH = "data/dblp_merge";
	private static final String nodes_csv = "/experiments/neo4j-community-3.1.1/import/nodes.csv";
	private static final String times_csv = "/experiments/neo4j-community-3.1.1/import/times.csv";
	private static final String connected_csv = "/experiments/neo4j-community-3.1.1/import/connected.csv";
	private static final String nodes_exist_csv = "/experiments/neo4j-community-3.1.1/import/exist.csv";
	private static Map<Integer, String> nodesNames;
	private static Map<Integer, Set<Integer>> nodes_times = new HashMap<>();
	private static Map<Integer, Map<Integer, Set<Integer>>> nodes = new HashMap<>();

	private static FileWriter acs;

	public static void main(String[] args) throws IOException {
		acs = new FileWriter(nodes_csv);
		loadDataset();
	}

	private static void loadDataset() throws IOException {

		BufferedReader br = new BufferedReader(new FileReader(DATASET_PATH));
		String line = null;
		String[] edge, time;
		int n1, n2;
		Set<Integer> time_instances = new HashSet<>();

		if (DATASET_PATH.contains("dblp")) {
			acs.write("id:ID(Node-ID),name,:LABEL\n");
			loadNames();
		} else
			acs.write("id:ID(Node-ID),:LABEL\n");

		// Set<Integer>
		long executionTime = System.currentTimeMillis();

		while ((line = br.readLine()) != null) {
			edge = line.split("\t");

			n1 = findCreateNode(Integer.parseInt(edge[0]));
			n2 = findCreateNode(Integer.parseInt(edge[1]));

			if (n1 == n2)
				continue;

			if (DATASET_PATH.contains("dblp")) {

				// edge[2] has the year/time
				time = edge[2].split(" ");

				for (String inter : time) {
					String[] in = inter.split(",");

					for (int i = Integer.parseInt(in[0]); i <= Integer.parseInt(in[1]); i++) {
						time_instances.add(i);
						nodes_times.get(n1).add(i);
						nodes_times.get(n2).add(i);

						Set<Integer> times;

						if ((times = nodes.get(n1).get(n2)) == null) {
							times = new HashSet<>();
							nodes.get(n1).put(n2, times);
						}

						times.add(i);
					}
				}
			} else {
				// fb dataset
				String[] token = edge[2].split(",");

				for (int time_instance = Integer.parseInt(token[0]); time_instance <= Integer
						.parseInt(token[1]); time_instance++) {
					time_instances.add(time_instance);
					nodes_times.get(n1).add(time_instance);
					nodes_times.get(n2).add(time_instance);

					Set<Integer> times;

					if ((times = nodes.get(n1).get(n2)) == null) {
						times = new HashSet<>();
						nodes.get(n1).put(n2, times);
					}

					times.add(time_instance);
				}
			}
		}
		br.close();

		createTimeInstancesIndex(time_instances);
		createAdjacencies();

		acs.close();

		System.out.println("Loadtime of all: " + (System.currentTimeMillis() - executionTime) + " (ms)");
	}

	private static void createAdjacencies() throws IOException {

		FileWriter w = new FileWriter(connected_csv);
		w.write(":START_ID(Node-ID),:END_ID(Node-ID),:TYPE\n");

		for (Entry<Integer, Map<Integer, Set<Integer>>> e : nodes.entrySet()) {

			for (Entry<Integer, Set<Integer>> e1 : e.getValue().entrySet()) {

				for (int t : e1.getValue()) {

					w.write(e.getKey() + "," + e1.getKey() + "," + t + "\n");

					if (nodes.get(e1.getKey()).get(e.getKey()) != null)
						nodes.get(e1.getKey()).get(e.getKey()).remove(t);
				}
			}
		}
		w.close();
	}

	private static void createTimeInstancesIndex(Set<Integer> time_instances) throws IOException {
		FileWriter y = new FileWriter(times_csv);
		y.write("timeInstance:ID(Time-ID),:LABEL\n");

		for (int year : time_instances)
			y.write(year + ",TimeInstance\n");

		y.close();

		y = new FileWriter(nodes_exist_csv);
		y.write(":START_ID(Time-ID),:END_ID(Node-ID),:TYPE\n");

		for (Entry<Integer, Set<Integer>> e : nodes_times.entrySet()) {

			for (int time_instance : e.getValue()) {
				y.write(time_instance + "," + e.getKey() + ",EXISTS\n");
			}
		}
		y.close();
	}

	private static int findCreateNode(int id) throws IOException {

		if (!nodes.containsKey(id)) {
			nodes.put(id, new HashMap<>());
			nodes_times.put(id, new HashSet<>());

			if (DATASET_PATH.contains("dblp"))
				acs.write(id + ",\"" + nodesNames.get(id) + "\",Author\n");
			else
				acs.write(id + ",User\n");
		}

		return id;
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