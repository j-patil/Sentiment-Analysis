import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import opennlp.tools.cmdline.PerformanceMonitor;
import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSSample;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.WhitespaceTokenizer;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;

public class StopwordsRemover {
	
	public static void sentencedetect (String i,String o) throws IOException{
		InputStream modelIn = new FileInputStream("lib/en-sent.bin");
		SentenceModel model = new SentenceModel(modelIn);
		if (modelIn != null)  
			modelIn.close(); 

		File inputFile = new File(i);
        FileReader fr = new FileReader(inputFile);
        BufferedReader br = new BufferedReader(fr);
        
        File outputFile = new File(o);
        FileWriter fw = new FileWriter(outputFile);
        BufferedWriter bw = new BufferedWriter(fw);
		SentenceDetectorME sentenceDetector = new SentenceDetectorME(model);
        String input;
		while ((input = br.readLine()) != null) {
			String sentences[] = sentenceDetector.sentDetect(input);
			for (String s : sentences)
			{
				bw.write(s);
				bw.newLine();
			}
		}
		br.close();
		bw.close();
	}

	public static void checker (String i,String o) throws IOException{
		
		File inputFile = new File(i);
        FileReader fr = new FileReader(inputFile);
        BufferedReader br = new BufferedReader(fr);
        
        File outputFile = new File(o);
        FileWriter fw = new FileWriter(outputFile);
        BufferedWriter bw = new BufferedWriter(fw);
        
        ArrayList<String> wordsList = new ArrayList<String>();
	    Set<String> stopWordsSet = new HashSet<String>();
	    
	    Scanner sc = new Scanner(new File("src/Stopword.txt"));
	    while (sc.hasNext()) {
	    	stopWordsSet.add(sc.next());
	    }
	    sc.close();
	    
        String input,store =" ";
        
        while ((input = br.readLine()) != null)
        {
        	String alpha= input.trim().replaceAll("[^a-zA-Z0-9\\s]", "").replaceAll("\\s+", " "); //Punctuation are removed here
			String[] words = alpha.split(" ");
	    	
		    for(String word : words) {
		    String wordCompare = word.toLowerCase();
		    if(!stopWordsSet.contains(wordCompare))
		    wordsList.add(word);
		    }
		    
		    for (String str : wordsList) {
		    	store += str+" ";
		    }		    
		    bw.write(store);	
		    bw.newLine();
		    store = " ";
		    wordsList.clear();
        }
        
	    br.close();
		bw.close();
		
		System.out.println("Done");
		}
			
    public static void posTag(String i,String o) throws IOException {
    	
        POSModel model = new POSModelLoader().load(new File("lib/en-pos-maxent.bin"));
        PerformanceMonitor perfMon = new PerformanceMonitor(System.err, "sent");
        POSTaggerME tagger = new POSTaggerME(model);

        File inputFile = new File(i);
        FileReader fr = new FileReader(inputFile);
        BufferedReader br = new BufferedReader(fr);

        File outputFile = new File(o);
        FileWriter fw = new FileWriter(outputFile);
        BufferedWriter bw = new BufferedWriter(fw);
        String input;
        
        perfMon.start();
        while ((input = br.readLine()) != null) {
            ObjectStream<String> lineStream = new PlainTextByLineStream(new StringReader(input));

            String line;
            while ((line = lineStream.read()) != null) {

                String whitespaceTokenizerLine[] = WhitespaceTokenizer.INSTANCE.tokenize(line);
                String[] tags = tagger.tag(whitespaceTokenizerLine);

                POSSample sample = new POSSample(whitespaceTokenizerLine, tags);
                String s = sample.toString();
                bw.write(s);
                bw.newLine();

                perfMon.incrementCounter();
            }
        }
            perfMon.stopAndPrintFinalResult();
            bw.close();
            br.close();
    }
       
	public static void main(String[] args) {
		try {
			StopwordsRemover.sentencedetect("testing/test.txt","testing/op.txt");
			StopwordsRemover.checker("testing/op.txt","testing/op2.txt");
			StopwordsRemover.posTag("testing/op2.txt","testing/opfinal.txt");
			}
		catch (IOException e) {	e.printStackTrace(); }
	}
}