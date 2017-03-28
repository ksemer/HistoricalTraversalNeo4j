package system;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import multiTypeEdge.MultiTraversal;
import singleTypeIntervalEdge.SingleIntervalTraversal;
import singleTypePointsEdge.SinglePointsTraversal;

public class Queries {

	private static GraphDatabaseService graphDb;
	public static String[] intervals;

	private static final String NEO4J_PATH = "/experiments/neo4j-community-3.1.1/";
	private static final File DB_PATH = new File(NEO4J_PATH + "data/databases/dblp_multi.db");
	private static final Label nodeLabel = Label.label("Author");
	public static final Label timeNodeLabel = Label.label("TimeInstance");
	public static final String timeNodeProperty = "timeInstance";

	private static final String QUERIES_PATH = "queriesReach";
	public static final boolean TIME_INDEX_ENABLED = true;
	public static final boolean simplePath = false;
	private static int traversalType = 2;

	public static void main(String[] args) throws NumberFormatException, IOException {
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(DB_PATH)
				.loadPropertiesFromFile(NEO4J_PATH + "conf/adbis.conf").newGraphDatabase();
		registerShutdownHook(graphDb);

		runQueries(QUERIES_PATH);
		shutdown();
	}

	private static void runQueries(String queriesPath) throws NumberFormatException, IOException {

		BufferedReader br = new BufferedReader(new FileReader(queriesPath));
		FileWriter writer = new FileWriter(queriesPath + "_result");
		String line = null, resultPath = null;
		String[] token = null, interval = null;
		Node src = null, trg = null;
		long tStart;
		boolean result = false;
		int c = 0;

		System.out.println("Queries started");
		writer.write("Result\tTime\n");

		GraphDbTraversal algo = null;

		if (traversalType == 1)
			algo = new SinglePointsTraversal(graphDb);
		else if (traversalType == 2)
			algo = new MultiTraversal(graphDb);
		else if (traversalType == 3)
			algo = new SingleIntervalTraversal(graphDb);

		try (Transaction tx = graphDb.beginTx()) {

			while ((line = br.readLine()) != null) {

				// token[0] has the query type
				token = line.split(",");
				System.out.println("Query: " + ++c);

				// query type
				int type = Integer.parseInt(token[0]);

				if (type < 7) {
					intervals = token[3].split(";");
					src = graphDb.findNode(nodeLabel, "id", token[1]);
					trg = graphDb.findNode(nodeLabel, "id", token[2]);
				} else
					intervals = token[1].split(";");

				tStart = System.currentTimeMillis();

				Set<Integer> times = new HashSet<>();

				for (String inter : intervals) {
					interval = inter.split("-");

					for (int t = Integer.parseInt(interval[0]); t <= Integer.parseInt(interval[1]); t++) {
						times.add(t);
					}
				}

				if (type == 1)
					result = algo.conjunctiveReachability(src, trg, times);
				else if (type == 2)
					result = algo.disjunctiveReachability(src, trg, times);
				else if (type == 3)
					result = algo.atLeastReachability(src, trg, times, Integer.parseInt(token[4]));
				else if (type == 4)
					resultPath = algo.conjunctivePath(src, trg, times);
				else if (type == 5)
					resultPath = algo.disjunctivePath(src, trg, times);
				else if (type == 6)
					resultPath = algo.atLeastPath(src, trg, times, Integer.parseInt(token[4]));
				else if (type == 7)
					resultPath = algo.conjunctiveReachability(times);
				else if (type == 8)
					resultPath = algo.disjunctiveReachability(times);
				else if (type == 9)
					resultPath = algo.atLeastReachability(times, Integer.parseInt(token[2]));

				if (type < 4)
					writer.write(result + "\t" + (System.currentTimeMillis() - tStart) + "\n");
				else
					writer.write(resultPath + "\t" + (System.currentTimeMillis() - tStart) + "\n");

				writer.flush();
			}
			br.close();

			tx.success();
		}
		writer.close();
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