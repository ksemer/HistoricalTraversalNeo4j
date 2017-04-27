package utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 
 * @author ksemer
 *
 */
public class PairsConf {

	private static final boolean sameConf = true;
	private static final String path = "dblp_authors_conf";
	private static final String outputpath = "soda-soda_q";
	private static final int numOfPairs = 500;
	private static final int K = 5;
	private static final String range = ",1987-2016,";
	private static final String conference = "SODA";
	// second conference when sameConf = false
	private static final String conference_ = "STOC";

	public static void main(String[] args) throws IOException {

		if (sameConf)
			runSameConf();
		else
			runPairConf();
	}

	/**
	 * Pair of queries at least K with nodes from two different conferences
	 * 
	 * @throws IOException
	 */
	private static void runPairConf() throws IOException {

		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = null;
		String[] token;

		Set<String> nodes1 = new HashSet<>(), nodes2 = new HashSet<>();

		while ((line = br.readLine()) != null) {
			token = line.split("\t");

			if (token[1].equals(conference))
				nodes1.add(token[0]);
			else if (token[1].equals(conference_))
				nodes2.add(token[0]);
		}
		br.close();

		List<String> nodes1_ = new ArrayList<>(nodes1), nodes2_ = new ArrayList<>(nodes2);
		Collections.shuffle(nodes1_);
		Collections.shuffle(nodes2_);

		FileWriter w = new FileWriter(outputpath);

		System.out.println(conference + ":" + nodes1_.size());
		System.out.println(conference_ + ":" + nodes2_.size());

		int counter = 1;
		Set<String> checked = new HashSet<>();

		for (int k = 1; k <= K; k++) {
			while (counter <= numOfPairs) {
				int u = getRandomNumber(0, nodes1_.size() - 1);
				int v = getRandomNumber(0, nodes2_.size() - 1);

				if (u == v || checked.contains(u + "," + v) || checked.contains(v + "," + u))
					continue;

				checked.add(u + "," + v);
				w.write("3," + nodes1_.get(u) + "," + nodes2_.get(v) + range + k + "\n");
				counter++;
			}
			counter = 1;
		}
		w.close();
	}

	/**
	 * Pair of queries at least K with nodes for same conference
	 * 
	 * @throws IOException
	 */
	private static void runSameConf() throws IOException {

		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = null;
		String[] token;
		Set<String> nodes = new HashSet<>();

		while ((line = br.readLine()) != null) {
			token = line.split("\t");

			if (token[1].equals(conference))
				nodes.add(token[0]);
		}
		br.close();

		List<String> nodes_ = new ArrayList<>(nodes);
		Collections.shuffle(nodes_);

		FileWriter w = new FileWriter(outputpath);

		System.out.println(nodes_.size());
		int counter = 1;
		Set<String> checked = new HashSet<>();

		for (int k = 1; k <= K; k++) {
			while (counter <= numOfPairs) {
				int u = getRandomNumber(0, nodes_.size() - 1);
				int v = getRandomNumber(0, nodes_.size() - 1);

				if (u == v || checked.contains(u + "," + v) || checked.contains(v + "," + u))
					continue;

				checked.add(u + "," + v);
				w.write("3," + nodes_.get(u) + "," + nodes_.get(v) + range + k + "\n");
				counter++;
			}
			counter = 1;
		}
		w.close();
	}

	/**
	 * Get random number in range min - max
	 * 
	 * @param min
	 * @param max
	 * @return
	 */
	private static int getRandomNumber(int min, int max) {
		return ThreadLocalRandom.current().nextInt(min, max + 1);
	}
}