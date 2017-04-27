package system;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import multiTypeEdge.MultiTraversal;
import singleTypeIntervalEdge.SingleIntervalTraversal;
import singleTypePointsEdge.SinglePointsTraversal;
import system.GraphDbTraversal.RelTypes;

public class BenchmarkTimes {

	private static GraphDatabaseService graphDb;
	public static String[] intervals;
	private static Set<String> nodes, INTERVALS;

	public static void main(String[] args) throws NumberFormatException, IOException {
		Config.loadConfig();

		graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(Config.DB_PATH)
				.loadPropertiesFromFile(Config.DATABASE_CONFIG_PATH).newGraphDatabase();
		registerShutdownHook(graphDb);

		Config.RESULTS_PATH += "_index";
		getNodes(Config.QUERIES_PATH);
		runTest();
		shutdown();
	}

	private static void getNodes(String path) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = null;
		String[] token;
		nodes = new HashSet<>();
		INTERVALS = new HashSet<>();

		while ((line = br.readLine()) != null) {

			// token[0] has the query type
			token = line.split(",");
			nodes.add(token[1]);
			nodes.add(token[2]);
			INTERVALS.add(token[3]);
		}
		br.close();
	}

	private static void runTest() throws NumberFormatException, IOException {

		String[] interval = null;
		Node src = null;
		long tStart;
		System.out.println("Queries started");
		@SuppressWarnings("unused")
		GraphDbTraversal algo = null;

		if (Config.TRAVERSAL_TYPE == 1)
			algo = new SinglePointsTraversal(graphDb);
		else if (Config.TRAVERSAL_TYPE == 2)
			algo = new MultiTraversal(graphDb);
		else if (Config.TRAVERSAL_TYPE == 3)
			algo = new SingleIntervalTraversal(graphDb);

		try (Transaction tx = graphDb.beginTx()) {

			FileWriter writer = new FileWriter(Config.RESULTS_PATH + "_" + Config.TRAVERSAL_TYPE, false);

			for (String n : nodes) {
				for (String in : INTERVALS) {

					intervals = in.split(";");
					src = graphDb.findNode(Config.NODE_LABEL, "id", n);
					Set<Integer> times = new HashSet<>();

					for (String inter : intervals) {
						interval = inter.split("-");

						for (int t = Integer.parseInt(interval[0]); t <= Integer.parseInt(interval[1]); t++) {
							times.add(t);
						}
					}

					tStart = System.nanoTime();

					if (Config.TRAVERSAL_TYPE == 1)
						getTimesPoints(src, times);
					else if (Config.TRAVERSAL_TYPE == 2)
						getTimesME(src, times);
					else if (Config.TRAVERSAL_TYPE == 3)
						getTimesInterval(src, times);

					writer.write(times.size() + "\t" + (System.nanoTime() - tStart) + "\n");
				}
				writer.flush();
			}
			writer.close();
			tx.success();
		}
	}

	private static Set<Integer> getTimesME(Node n, Set<Integer> times) {
		Set<Integer> t = new HashSet<>(times);

		for (Relationship rel : n.getRelationships(RelTypes.EXISTS, Direction.INCOMING)) {
			t.remove(Integer.parseInt(rel.getStartNode().getProperty("timeInstance").toString()));
		}

		return t;
	}

	private static BitSet getTimesInterval(Node n, Set<Integer> times) {
		BitSet lifespan = new BitSet(), I = new BitSet();
		String[] interval;
		int start, end, j_ = 0;

		int[] tstart = (int[]) n.getProperty("tstart");
		int[] tend = (int[]) n.getProperty("tend");

		for (int i = 0; i < intervals.length; i++) {
			interval = intervals[i].split("-");
			start = Integer.parseInt(interval[0]);
			end = Integer.parseInt(interval[1]);

			I.set(start, end + 1, true);

			for (int j = j_; j < tstart.length; j++) {

				if (tend[j] < start) {
					if (j + 1 == tstart.length)
						break;

					continue;
				}

				if (tstart[j] > end)
					break;

				if (Math.max(tstart[j], start) <= Math.min(tend[j], end)) {

					// if query interval is subinterval of edge lifespan
					if (start >= tstart[j] && end <= tend[j])
						lifespan.set(start, end + 1);
					// if edge lifespan is subinterval of query interval
					else if (tstart[j] >= start && tend[j] <= end)
						lifespan.set(tstart[j], tend[j] + 1);
					else {
						for (int i_ = I.nextSetBit(tstart[j]); i_ >= 0; i_ = I.nextSetBit(i_ + 1)) {
							if (i_ >= tstart[j] && i_ <= tend[j])
								lifespan.set(i_);
						}
					}

					j_ = j;
				}
			}
		}

		lifespan.and(I);

		return lifespan;
	}

	private static BitSet getTimesPoints(Node n, Set<Integer> times) {
		BitSet lifespan = new BitSet();
		int[] timeInstances = (int[]) n.getProperty("time_instances");

		for (int time_instance : times) {
			if (Arrays.binarySearch(timeInstances, time_instance) >= 0)
				lifespan.set(time_instance);
		}
		return lifespan;
	}

	private static void shutdown() {
		graphDb.shutdown();
	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {

		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}
}