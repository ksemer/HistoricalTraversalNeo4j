package utils;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Read {
	private static String base = "synthetic_queries_with_dels/merge_sort/";
	
	public static void main(String[] args) throws IOException {
		read("10trav_noI_results_1");
		System.out.println("---");
//		read("path_results_1");
//		System.out.println("---");
//		read("trav_noI_results_3");
		
//		read("path_noI_results_1");
//		System.out.println("---");
//		read("path_noI_results_2");
//		System.out.println("---");
//		read("path_noI_results_3");
		
//		read("path_results_1");
//		System.out.println("---");
//		read("path_results_2");
//		System.out.println("---");
//		read("path_results_3");
	}
	
	public static void read(String file) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(base + file));
		String line = null;
		String[] token;
		
		int a[] = {0, 0, 0, 0, 0}, b[] = {0, 0, 0, 0, 0}, c[] = {0, 0, 0, 0, 0};
		int a_[] = {0, 0, 0, 0, 0}, b_[] = {0, 0, 0, 0, 0}, c_[] = {0, 0, 0, 0, 0};
		
		while ((line = br.readLine()) != null) {
			token = line.split("\t");
			
			if (token[0].equals("1") || token[0].equals("4")) {
				
				if (token[3].equals("0"))
					continue;
				if (token[1].equals("6")) {
					a[0]+= Integer.parseInt(token[3]);
					a_[0]++;
				}
				
				if (token[1].equals("12")) {
					a[1]+= Integer.parseInt(token[3]);
					a_[1]++;
				}
				
				if (token[1].equals("18")) {
					a[2]+= Integer.parseInt(token[3]);
					a_[2]++;
				}
				
				if (token[1].equals("24")) {
					a[3]+= Integer.parseInt(token[3]);
					a_[3]++;
				}
				
				if (token[1].equals("30")) {
					a[4]+= Integer.parseInt(token[3]);
					a_[4]++;
				}
			}
			
			if (token[0].equals("2") || token[0].equals("5")) {
				if (token[1].equals("6")) {
					b[0]+= Integer.parseInt(token[3]);
					b_[0]++;
				}
				
				if (token[1].equals("12")) {
					b[1]+= Integer.parseInt(token[3]);
					b_[1]++;
				}
				
				if (token[1].equals("18")) {
					b[2]+= Integer.parseInt(token[3]);
					b_[2]++;
				}
				
				if (token[1].equals("24")) {
					b[3]+= Integer.parseInt(token[3]);
					b_[3]++;
				}
				
				if (token[1].equals("30")) {
					b[4]+= Integer.parseInt(token[3]);
					b_[4]++;
				}
			}
			
			if (token[0].equals("3") || token[0].equals("6")) {
				if (token[1].equals("6")) {
					c[0]+= Integer.parseInt(token[3]);
					c_[0]++;
				}
				
				if (token[1].equals("12")) {
					c[1]+= Integer.parseInt(token[3]);
					c_[1]++;
				}
				
				if (token[1].equals("18")) {
					c[2]+= Integer.parseInt(token[3]);
					c_[2]++;
				}
				
				if (token[1].equals("24")) {
					c[3]+= Integer.parseInt(token[3]);
					c_[3]++;
				}
				
				if (token[1].equals("30")) {
					c[4]+= Integer.parseInt(token[3]);
					c_[4]++;
				}
			}
				
		}
		br.close();
		
		for (int i = 0; i < a.length; i++)
			System.out.println(a[i] / a_[i] + "\t" + b[i] / b_[i] + "\t" + c[i] / c_[i]);
	}
}