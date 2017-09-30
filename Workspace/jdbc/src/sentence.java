import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;

public class sentence {
	
	public static String[] sentenceDetect (String input) throws IOException {
		InputStream modelIn = new FileInputStream("/home/rutesh/workspace/Beta/lib/en-sent.bin");
		SentenceModel model = new SentenceModel(modelIn);
		if (modelIn != null)  
			modelIn.close(); 
		
		SentenceDetectorME sentenceDetector = new SentenceDetectorME(model);
        String sentences[] = sentenceDetector.sentDetect(input);
        return sentences;
	}

	public static String checker (String i) throws IOException {
	    
		ArrayList<String> phrasein = new ArrayList<String>();
		ArrayList<String> phraseout = new ArrayList<String>();
		ArrayList<String> wordsList = new ArrayList<String>();
	    Set<String> stopWordsSet = new HashSet<String>();
		Scanner in,out;
		String alpha = i.trim().replaceAll("[^a-zA-Z0-9\\s]", "").replaceAll("\\s+", " ");
		in = new Scanner(new File("/home/rutesh/workspace/Beta/lib/phrasein.txt"));
		out = new Scanner(new File("/home/rutesh/workspace/Beta/lib/phraseout.txt"));
		String store = "";
    	while (in.hasNextLine()) {
    	  phrasein.add(in.nextLine());
    	}
    	in.close();
    	
    	while (out.hasNextLine()) {
    	  phraseout.add(out.nextLine());
    	}
    	out.close();
    	
    	String[] arrin = phrasein.toArray(new String[0]);
    	String[] arrout = phraseout.toArray(new String[0]);
    	
    	String beta = null;
    	for(int k=0;k<arrin.length;k++){
    		beta = alpha.replaceAll(arrin[k],arrout[k]);
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
	    store = " ";
	    wordsList.clear();
	    return t;
    }

	public static void main (String args[]) {
		try {
		File inputFile = new File("/home/rutesh/Desktop/hadoop.txt");
		FileReader fr = new FileReader(inputFile);
		BufferedReader br = new BufferedReader(fr);
		
		File outputFile = new File("op.txt");
		FileWriter fw = new FileWriter(outputFile);
		BufferedWriter bw = new BufferedWriter(fw);
		
		String line,sentence[];
		while((line = br.readLine())!= null) {
		sentence = sentenceDetect(line);
		for (String s : sentence) {
			s = checker(s);
			bw.write(s);
			bw.newLine();
			//System.out.println(s);
		}
		}
		br.close();
		bw.close();
		System.out.println("Done");
		}
		catch(Exception e) {}
	}
}
