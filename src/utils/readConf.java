package utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class readConf {
	private static final String path = "conferences/soda-soda_trav_results_2";

	public static void main(String[] args) throws IOException {
		System.out.println("TRUE\tFALSE\tTOTAL");

		for (int k = 1; k <= 5; k++)
			run(k);
	}

	private static void run(int k) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(path));
		String line = null;
		String[] token;
		int count_true = 0, count_false = 0, count_all = 0, count = 0;

		while ((line = br.readLine()) != null) {
			count_all++;

			if (count_all > k * 500)
				break;

			if (count_all < ((k - 1) * 500) + 1)
				continue;

			if (line.contains("error"))
				continue;

			token = line.split("\t");

			if (token[2].equals("false")) {
				count_false++;
			} else if (token[2].equals("true")) {
				count_true++;
			}

			count++;
		}
		br.close();

		System.out.println(count_true + "\t" + count_false + "\t" + count);
	}
}