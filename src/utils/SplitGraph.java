package utils;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SplitGraph {
	private static final String path = "graph1";
	private static final int num = 1000000;

	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(path));
		FileWriter w = new FileWriter(path + "_s" + (num / 100000));
		String line = null;
		String[] token;
		Set<Integer> nodes = new HashSet<Integer>();
		
		while ((line = br.readLine()) != null) {
			token = line.split("\t");
			nodes.add(Integer.parseInt(token[0]));
			nodes.add(Integer.parseInt(token[1]));
			
			if (nodes.size() > num)
				break;
			
			w.write(line + "\n");	
		}
		br.close();
		w.close();
	}
}