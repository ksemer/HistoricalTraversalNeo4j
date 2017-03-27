import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class QueriesGenerator {
	private static final int queries = 50;
	private static final int queries_types = 3;
	private static final int maximumInstance = 2016;
	private static final int minimumInstance = 2000;
	private static final String INPUT_PATH = "data/dblp";
	private static final String OUTPUT_PATH = "queries";

	private static int randInt(int max, int min) {

		// Usually this can be a field rather than a method variable
		Random rand = new Random();

		// nextInt is normally exclusive of the top value,
		// so add 1 to make it inclusive
		int randomNum = rand.nextInt(max - min) + min;

		return randomNum;
	}

	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(INPUT_PATH));
		String line = null;
		Set<Integer> authors = new HashSet<>();
		String[] token;

		while ((line = br.readLine()) != null) {
			token = line.split("\t");
			
			authors.add(Integer.parseInt(token[0]));
			authors.add(Integer.parseInt(token[1]));
		}
		br.close();
		System.out.println("Set size: " + authors.size());

		List<Integer> author_s = new ArrayList<>(authors);
		authors = null;

		System.out.println("List size: " + author_s.size());

		int author_1, author_2,from, to, type, k;
		FileWriter writer = new FileWriter(OUTPUT_PATH);

		for (int i = 0; i < queries_types * queries; i++) {
			// query type
			type = (i / queries) + 1;

			do {
				author_1 = author_s.get(randInt(author_s.size(), 0));
				author_2 = author_s.get(randInt(author_s.size(), 0));
			} while (author_1 == author_2);

			if (type == 1 || type == 2) {
				do {
					from = randInt(maximumInstance + 1, minimumInstance);
				} while (from == maximumInstance);

				to = randInt(maximumInstance + 1, from + 1);
				writer.write(type + "," + author_1 + "," + author_2 + "," + from + "-" + to + "\n");
			} else if (type == 3) {
				do {
					from = randInt(maximumInstance + 1, minimumInstance);
				} while (from >= maximumInstance - 1);

				do {
					to = randInt(maximumInstance + 1, from + 1);
				} while (to - from < 2);

				k = randInt(to - from + 2, 2);
				writer.write(type + "," + author_1 + "," + author_2 + "," + from + "-" + to + "," + k + "\n");
			}
			writer.flush();
		}
		writer.close();
	}
}