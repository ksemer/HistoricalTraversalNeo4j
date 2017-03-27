import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.io.fs.FileUtils;

public class Benchmark {
	private static final File DB_PATH = new File("/experiments/neo4j-community-3.1.1/data/databases/neo4j-store");
	private static Map<Integer, Set<Integer>> time_instances = new HashMap<>();
	private static TreeMap<Integer, Set<Integer>> query_intervals = new TreeMap<>();
	private static final int min = 1;
	private static final int max = 1000;
	private static boolean createDatabase = false;
	private static GraphDatabaseService graphDb;
	private static Label label = Label.label("Node");
	private static Label labeltime = Label.label("Time");

	public static void main(final String[] args) throws IOException {

		FileWriter w = new FileWriter("bench_res");

		for (int i = 10; i <= 100; i += 10) {
			Set<Integer> set = new HashSet<>();

//			while (set.size() == 0) {
//				int x = getRandomNumber(min, max);
//				if (x + i <= max)
//					set.add(x);
//			}
			
			if (i == 10) {
				set.add(2);
			}
			
		if (i == 20) {
			set.add(123);
	
			}
		
		if (i == 30) {
			set.add(610);

		}
		
		if (i == 40) {
			set.add(416);

		}
		
		if (i == 50) {
			set.add(387);

		}
		
		if (i == 60) {
			set.add(656);

		}
		
		if (i == 70) {
			set.add(387);

		}
		if (i == 80) {
			set.add(764);

		}
		if (i == 90) {
			set.add(765);

		}
		if (i == 100) {
			set.add(813);

		}
			query_intervals.put(i, set);
			System.out.println(set);
		}

		if (createDatabase)
			createDatabase();

		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);

		try (Transaction tx = graphDb.beginTx()) {
			for (IndexDefinition x : graphDb.schema().getIndexes()) {
				System.out.println(x.getPropertyKeys() + "\t" + x.getLabel());
			}
			tx.success();
		}
		
//		try (Transaction tx = graphDb.beginTx()) {
//
//			for (int id = 1; id <= 10; id++) {
//
//				w.write(newModeEdge(id) + "\n");
//			}
//
//			tx.success();
//		}

		
//		w.write("\n\n");
//		try (Transaction tx = graphDb.beginTx()) {
//
//			for (int id = 1; id <= 10; id++) {
//
//				w.write(multiEdge(id) + "\n");
//			}
//
//			tx.success();
//		}
//
//		w.write("\n\n");
		try (Transaction tx = graphDb.beginTx()) {
			for (int id = 1; id <= 10; id++) {
				w.write(pointsBench(id) + "\n");
			}

			tx.success();
		}


		w.close();
	}

	private static String newModeEdge(int id) {
		Node n = graphDb.findNode(label, "nID", id);
		String res = "" + (id * 100);

		for (Entry<Integer, Set<Integer>> e : query_intervals.entrySet()) {
			int k = e.getKey();
			int s = e.getValue().iterator().next();

			long time = System.nanoTime();

			int[] start = (int[]) n.getProperty("lifespan_start");
			int[] end = (int[]) n.getProperty("lifespanEnd");

			Set<Integer> join = new HashSet<>();

			int i = s, stop = s + k - 1;
			for (int j = 0; j < start.length; j++) {
				if (end[j] < i)
					continue;

				if (start[j] > stop)
					break;

				if (start[j] <= i && end[j] >= i) {
					join.add(i);

					for (int q = i + 1; q <= stop; q++) {
						if (end[j] >= q)
							join.add(q);
						else {
							i = q;
							break;
						}
					}
				} else {
					i = start[j];
					
					if (i > stop)
						break;
					
					j--;
				}
			}

			res += "\t" + (System.nanoTime() - time)/* + "--" + join*/;
		}

		return res;
	}

	private static String multiEdge(int id) {
		Node n = graphDb.findNode(label, "nID", id);
		String res = "" + (id * 100);

		for (Entry<Integer, Set<Integer>> e : query_intervals.entrySet()) {
			int k = e.getKey();
			int start = e.getValue().iterator().next();

			long time = System.nanoTime();

			Set<Integer> join = new HashSet<>();

			for (int i = start; i < start + k; i++) {
				if (n.getSingleRelationship(RelationshipType.withName("" + i), Direction.OUTGOING) != null)
					join.add(i);
			}

			res += "\t" + (System.nanoTime() - time)/* + "--" + join*/;
		}

		return res;
	}

	private static String pointsBench(int id) {
		Node n = graphDb.findNode(label, "nID", id);
		String res = "" + (id * 100);

		for (Entry<Integer, Set<Integer>> e : query_intervals.entrySet()) {
			int k = e.getKey();
			int start = e.getValue().iterator().next();

			long time = System.nanoTime();

			int[] timepoints = (int[]) n.getProperty("lifespanPoints");
			Set<Integer> p = new HashSet<Integer>();
			Set<Integer> join = new HashSet<>();
			
//			int j = start, stop = start + k;
//			for (int i = 0; i < timepoints.length; i++) {
//				if (timepoints[i] < start)
//					continue;
//				
//				if (timepoints[i] > stop)
//					break;
//				
//				if (timepoints[i] >= start && timepoints[i] < stop) {
//					join.add(timepoints[i]);
//				}
//			}

			for (int i = start; i < start + k; i++) {
				p.add(i);
			}

			for (int i : timepoints) {
				if (p.contains(i))
					join.add(i);
			}

			res += "\t" + (System.nanoTime() - time)/* + "--" + join*/;
		}

		return res;
	}

	private static void createDatabase() throws IOException {
		System.out.println("Starting database ...");
		FileUtils.deleteRecursively(DB_PATH);

		for (int i = 100; i <= max; i += 100) {
			Set<Integer> set = new HashSet<>();

			while (set.size() < i) {
				set.add(getRandomNumber(min, max));
			}
			time_instances.put(i, set);
			System.out.println(i + "\t" + set);
		}

		GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);

		IndexDefinition indexDefinition, indexTime;

		try (Transaction tx = graphDb.beginTx()) {
			Schema schema = graphDb.schema();
			indexDefinition = schema.indexFor(label).on("nID").create();
			indexTime = schema.indexFor(labeltime).on("nID").create();
			tx.success();
		}

		try (Transaction tx = graphDb.beginTx()) {
			Schema schema = graphDb.schema();
			schema.awaitIndexOnline(indexDefinition, 10, TimeUnit.SECONDS);
			schema.awaitIndexOnline(indexTime, 10, TimeUnit.SECONDS);
		}

		try (Transaction tx = graphDb.beginTx()) {
			Schema schema = graphDb.schema();
			System.out.println(String.format("Percent complete: %1.0f%%",
					schema.getIndexPopulationProgress(indexDefinition).getCompletedPercentage()));
			System.out.println(String.format("Percent complete: %1.0f%%",
					schema.getIndexPopulationProgress(indexTime).getCompletedPercentage()));
		}

		try (Transaction tx = graphDb.beginTx()) {

			// create timeNode
			Node n = graphDb.createNode(labeltime);
			n.setProperty("nID", 0);

			// Create nodes
			for (int id = 1; id <= 10; id++) {
				int time = id * 100;
				Set<Integer> times = time_instances.get(time);

				Node node = graphDb.createNode(label);
				node.setProperty("nID", id);

				Integer[] timesArray = times.toArray(new Integer[times.size()]);
				Arrays.sort(timesArray);

				for (int i : timesArray) {
					node.createRelationshipTo(n, RelationshipType.withName("" + i));
				}

				node.setProperty("lifespanPoints", timesArray);

				String intervals = getIntervals(timesArray);
				String[] token = intervals.split(" ");
				int[] start = new int[token.length], end = new int[token.length];

				for (int i = 0; i < token.length; i++) {
					String[] life = token[i].split(",");
					start[i] = Integer.parseInt(life[0]);
					end[i] = Integer.parseInt(life[1]);
				}
				node.setProperty("lifespan_start", start);
				node.setProperty("lifespanEnd", end);
			}
			System.out.println("Nodes created");

			tx.success();
		}
		System.out.println("Shutting down database ...");
		graphDb.shutdown();
	}

	private static int getRandomNumber(int min, int max) {
		return ThreadLocalRandom.current().nextInt(min, max + 1);
	}

	private static String getIntervals(Integer[] timesArray) {

		int start = timesArray[0], end = start;
		String result = "";

		for (int i = 1; i < timesArray.length; i++) {
			if (timesArray[i] == end + 1)
				end++;
			else {
				result += start + "," + end + " ";
				start = timesArray[i];
				end = start;
			}
		}

		if (start == end || result.isEmpty() || (!result.isEmpty() && start < end))
			result += start + "," + end;

		return result;
	}
}