package jdbc;

import java.util.StringTokenizer;

public class exp {

	public static void main(String args[]) {
		String s = "This is sentence one,This is sentence two";
		StringTokenizer isr = new StringTokenizer(s,",");
		while(isr.hasMoreTokens()) {
			StringTokenizer big = new StringTokenizer(isr.nextToken());
			while(big.hasMoreTokens()){
			System.out.println(big.nextToken());
			}
		}
	}
}
