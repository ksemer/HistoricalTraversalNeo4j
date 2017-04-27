package system;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import multiTypeEdge.MultiTraversal;
import singleTypeIntervalEdge.SingleIntervalTraversal;
import singleTypePointsEdge.SinglePointsTraversal;

/**
 * Queries class
 * 
 * @author ksemer
 */
public class Queries {

	private static GraphDatabaseService graphDb;
	public static String[] intervals;

	public static void main(String[] args) throws NumberFormatException, IOException {
		Config.loadConfig();

		graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(Config.DB_PATH)
				.loadPropertiesFromFile(Config.DATABASE_CONFIG_PATH).newGraphDatabase();
		registerShutdownHook(graphDb);

		if (Config.RUN_PATH_QUERIES)
			Config.RESULTS_PATH += "path_";
		else
			Config.RESULTS_PATH += "trav_";

		if (!Config.TIME_INDEX_ENABLED)
			Config.RESULTS_PATH += "noI_";

		Config.RESULTS_PATH += "results";

		runQueries(Config.QUERIES_PATH);
		shutdown();
	}

	private static void runQueries(String queriesPath) throws NumberFormatException, IOException {

		BufferedReader br = new BufferedReader(new FileReader(queriesPath));
		String line = null, resultPath = null;
		String[] token = null, interval = null;
		Node src = null, trg = null;
		long tStart;
		boolean result = false;
		int c = 0;

		System.out.println("Queries started");
		GraphDbTraversal algo = null;

		if (Config.TRAVERSAL_TYPE == 1)
			algo = new SinglePointsTraversal(graphDb);
		else if (Config.TRAVERSAL_TYPE == 2)
			algo = new MultiTraversal(graphDb);
		else if (Config.TRAVERSAL_TYPE == 3)
			algo = new SingleIntervalTraversal(graphDb);

		try (Transaction tx = graphDb.beginTx()) {

			FileWriter writer = new FileWriter(Config.RESULTS_PATH + "_" + Config.TRAVERSAL_TYPE, true);

			while ((line = br.readLine()) != null) {

				// token[0] has the query type
				token = line.split(",");
				System.out.println("Query: " + ++c);

				// query type
				int type = Integer.parseInt(token[0]);

				if (type < 7) {
					intervals = token[3].split(";");
					src = graphDb.findNode(Config.NODE_LABEL, "id", token[1]);
					trg = graphDb.findNode(Config.NODE_LABEL, "id", token[2]);
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

				if (Config.RUN_PATH_QUERIES)
					type += 3;

				try {

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
				} catch (Exception e) {
					writer.write("error\n");
				}

				if (type < 4)
					writer.write(type + "\t" + times.size() + "\t" + result + "\t"
							+ (System.currentTimeMillis() - tStart) + "\n");
				else
					writer.write(type + "\t" + times.size() + "\t" + resultPath + "\t"
							+ (System.currentTimeMillis() - tStart) + "\n");

				writer.flush();
			}
			br.close();
			writer.close();
			tx.success();
		}
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