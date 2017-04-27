package utils;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Queries Generator class
 * 
 * @author ksemer
 */
public class QueriesGenerator {

	private static final int queries = 50;
	private static final boolean pathQueries = false;
	private static final int queries_types = 3;
	private static final int start = 10;
	private static final int end = 10;
	private static final int maximumInstance = 99;
	private static final int minimumInstance = 0;
	private static final String INPUT_PATH = "graph1_50";
	private static final String OUTPUT_PATH = "graph_queries_50";
	private static Map<Integer, BitSet> nodes = new HashMap<>();
	private static FileWriter writer;

	public static void main(String[] args) throws IOException {
		loadDataset();
		writer = new FileWriter(OUTPUT_PATH);

		for (int i = start; i <= end; i += start) {
			generate(i);
		}

		writer.close();
	}

	private static void generate(int range) throws IOException {

		int u, v;

		System.out.println("Queries Generator started");

		List<Integer> nodes_ = new ArrayList<>(nodes.keySet());
		int type, from, to;
		boolean stop;

		for (int i = 0; i < queries_types * queries; i++) {
			// query type
			type = (i / queries) + 1;

			do {
				stop = false;

				do {
					from = getRandomNumber(minimumInstance, maximumInstance);
				} while (from == maximumInstance || (from + range - 1) > maximumInstance);

				to = from + range - 1;

				do {
					u = nodes_.get(getRandomNumber(0, nodes.size() - 1));
					v = nodes_.get(getRandomNumber(0, nodes.size() - 1));
				} while (u == v);

				if (alive(u, from, to) && alive(v, from, to))
					stop = true;

			} while (!stop);

			if (type != 3) {
				if (pathQueries) {
					if (type == 1)
						writer.write("4," + u + "," + v + "," + from + "-" + to + "\n");
					else if (type == 2)
						writer.write("5," + u + "," + v + "," + from + "-" + to + "\n");
				} else
					writer.write(type + "," + u + "," + v + "," + from + "-" + to + "\n");
			} else {
				if (pathQueries)
					writer.write("6," + u + "," + v + "," + from + "-" + to + "," + range / 2 + "\n");
				else
					writer.write(type + "," + u + "," + v + "," + from + "-" + to + "," + range / 2 + "\n");
			}

			writer.flush();
		}
	}

	private static boolean alive(int u, int from, int to) {
		if (nodes.get(u).get(from) && nodes.get(u).get(to))
			return true;

		return false;
	}

	private static int getRandomNumber(int min, int max) {
		return ThreadLocalRandom.current().nextInt(min, max + 1);
	}

	private static void updateLifespan(BitSet life, String[] token) {
		String[] time;

		for (String inter : token) {
			time = inter.split(",");

			life.set(Integer.parseInt(time[0]), Integer.parseInt(time[1]) + 1, true);
		}
	}

	private static void loadDataset() throws IOException {
		System.out.println("Dataset is loading");

		BufferedReader br = new BufferedReader(new FileReader(INPUT_PATH));
		String line = null;
		String[] token;
		int u, v;
		BitSet life;

		while ((line = br.readLine()) != null) {
			token = line.split("\t");
			u = Integer.parseInt(token[0]);
			v = Integer.parseInt(token[1]);
			token = token[2].split(" ");

			if ((life = nodes.get(u)) == null) {
				life = new BitSet();
				nodes.put(u, life);
			}

			updateLifespan(life, token);

			if ((life = nodes.get(v)) == null) {
				life = new BitSet();
				nodes.put(v, life);
			}

			updateLifespan(life, token);
		}
		br.close();

		System.out.println("Dataset has loaded");
	}
}