package utils;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class CreateGraphWithDeletions {
	private static final String path = "graph1";
	private static final double percentageDel = 50;

	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(path));
		FileWriter w = new FileWriter(path + "_" + (int) percentageDel);
		String line = null;
		String[] token, inter;
		BitSet lifespan;
		List<Integer> life;

		while ((line = br.readLine()) != null) {
			token = line.split("\t");
			inter = token[2].split(",");
			lifespan = new BitSet();
			life = new ArrayList<>();

			for (int i = Integer.parseInt(inter[0]); i <= Integer.parseInt(inter[1]); i++) {
				life.add(i);
			}

			Collections.shuffle(life);

			int d = (int) (life.size() * percentageDel / 100);

			if (d == 0)
				continue;

			for (int i = 0; i < life.size() - d; i++)
				lifespan.set(life.get(i));

			if (!lifespan.isEmpty()) {

				w.write(token[0] + "\t" + token[1] + "\t" + getIntervals(lifespan) + "\n");
			}
		}
		br.close();
		w.close();
	}

	private static String getIntervals(BitSet life) {

		int start = life.nextSetBit(0), end = start, i;
		String result = "";
		life.set(start, false);

		for (Iterator<Integer> it = life.stream().iterator(); it.hasNext();) {
			i = it.next();

			if (i == end + 1)
				end++;
			else {
				result += start + "," + end + " ";
				start = i;
				end = start;
			}
		}

		if (start == end || result.isEmpty() || (!result.isEmpty() && start < end))
			result += start + "," + end;

		return result;
	}
}