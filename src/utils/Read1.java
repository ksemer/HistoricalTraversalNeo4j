package utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Read1 {
	private static String base = "";

	public static void main(String[] args) throws IOException {

		read("_index_1");
		System.out.println("---");
		read("_index_2");
		System.out.println("---");
		read("_index_3");
		System.out.println("---");
	}

	public static void read(String file) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(base + file));
		String line = null;
		String[] token;

		long a[] = { 0, 0, 0, 0, 0 };
		long a_[] = { 0, 0, 0, 0, 0 };

		while ((line = br.readLine()) != null) {
			token = line.split("\t");

			if (line.isEmpty())
				continue;

			if (token[0].equals("0"))
				continue;

			if (token[0].equals("6")) {
				a[0] += Integer.parseInt(token[1]);
				a_[0]++;
			}

			if (token[0].equals("12")) {
				a[1] += Integer.parseInt(token[1]);
				a_[1]++;
			}

			if (token[0].equals("18")) {
				a[2] += Integer.parseInt(token[1]);
				a_[2]++;
			}

			if (token[0].equals("24")) {
				a[3] += Integer.parseInt(token[1]);
				a_[3]++;
			}

			if (token[0].equals("30")) {
				a[4] += Integer.parseInt(token[1]);
				a_[4]++;
			}
		}
		br.close();

		for (int i = 0; i < a.length; i++) {
			if (a_[i] == 0)
				a_[i]++;

			System.out.println(a[i] / a_[i]);
		}
	}
}