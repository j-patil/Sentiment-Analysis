package jdbc;

	public class ex {

		public static void main(String[] args) {
			try {
			ProcessBuilder proc = new ProcessBuilder("/home/rutesh/workspace/jdbc/src/jdbc/Untitled.sh");
			Process p = proc.start();
			p.waitFor();
			System.out.println("Script Executed Sucessfully");
			} catch (Exception e) { e.printStackTrace(); }
		}
	}