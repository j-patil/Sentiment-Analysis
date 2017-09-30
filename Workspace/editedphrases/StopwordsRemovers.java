import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

public class StopwordsRemovers {

public static void checker (String i,String o) throws IOException{
		
		File inputFile = new File(i);
        FileReader fr = new FileReader(inputFile);
        BufferedReader br = new BufferedReader(fr);
        
        File outputFile = new File(o);
        FileWriter fw = new FileWriter(outputFile);
        BufferedWriter bw = new BufferedWriter(fw);
        
        ArrayList<String> wordsList = new ArrayList<String>();
	    Set<String> stopWordsSet = new HashSet<String>();
	    
	    Scanner sc = new Scanner(new File("C:\\Users\\Rahul\\Desktop\\Stopword.txt"));
	    while (sc.hasNext()) {
	    	stopWordsSet.add(sc.next());
	    }
	    sc.close();
	    
	    
        String input,store =" ";
        
        while ((input = br.readLine()) != null)
        {
        	String alpha= input.trim().replaceAll("[^a-zA-Z0-9\\s]", "").replaceAll("\\s+", " "); //Punctuation are removed here
        	
        	Scanner s = new Scanner(new File("C:\\Users\\Rahul\\Desktop\\phrase.txt"));
        	ArrayList<String> lines = new ArrayList<String>();
        	while (s.hasNextLine()) {
        	  lines.add(s.nextLine());
        	}
        	s.close();
        	String[] arr = lines.toArray(new String[0]);
        	
        	Scanner s1 = new Scanner(new File("C:\\Users\\Rahul\\Desktop\\phraseout.txt"));
        	ArrayList<String> lines1 = new ArrayList<String>();
        	while (s1.hasNextLine()) {
        	  lines1.add(s1.nextLine());
        	}
        	s1.close();
        	String[] arrout = lines1.toArray(new String[0]);
        	
        	String beta = null;
        	for(int k=0;k<arr.length;k++){
        		beta = alpha.replaceAll(arr[k],arrout[k]);
        	    alpha=beta;
        	}
    	    
			String[] words = beta.split(" ");
	    	
		    for(String word : words) {
		    String wordCompare = word.toLowerCase();
		    if(!stopWordsSet.contains(wordCompare))
		    wordsList.add(word);
		    }
		    
		    for (String str : wordsList) {
		    	store += str+" ";
		    }	
		    String t = store.trim().toLowerCase();
		    bw.write(t);	
		    bw.newLine();
		    store = " ";
		    wordsList.clear();
        }
        
	    br.close();
		bw.close();
		
		System.out.println("Done");
		}
			
    public static void main(String[] args) {
	try {
		StopwordsRemovers.checker("C:\\Users\\Rahul\\Desktop\\input.txt","C:\\Users\\Rahul\\Desktop\\output.txt");
		}
	catch (IOException e) {	e.printStackTrace(); }
	}
	
}
