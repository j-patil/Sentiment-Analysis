import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

class ex {

	public static void main(String[] args) throws IOException, InterruptedException {

		String command = "ping -c 3 www.google.com";
		Process proc = Runtime.getRuntime().exec(command);
		BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
		String line = "";
		while ((line = reader.readLine()) != null) {
			System.out.print(line + "\n");
		}
		proc.waitFor();
		}
	}
