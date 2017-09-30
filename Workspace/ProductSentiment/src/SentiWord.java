import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import MapReduceTwo.KeyMapper;
import MapReduceTwo.KeyReducer;

    public class SentiWord {
        private String pathToSWN ="/home/rutesh/workspace/ProductSentiment/SentiWordNet_3.0.0.txt";
        private HashMap<String, Double> dict;

        public SentiWord(){

            dict = new HashMap<String, Double>();
            HashMap<String, Vector<Double>> _temp = new HashMap<String, Vector<Double>>();
            try{
                BufferedReader csv =  new BufferedReader(new FileReader(pathToSWN));
                String line = "";           
                while((line = csv.readLine()) != null)
                {
                    String[] data = line.split("\t");
                    Double score = Double.parseDouble(data[2])-Double.parseDouble(data[3]);
                    String[] words = data[4].split(" ");
                    for(String w:words)
                    {
                        String[] w_n = w.split("#");
                        w_n[0] += "#"+data[0];
                        int index = Integer.parseInt(w_n[1])-1;
                        if(_temp.containsKey(w_n[0]))
                        {
                            Vector<Double> v = _temp.get(w_n[0]);
                            if(index>v.size())
                                for(int i = v.size();i<index; i++)
                                    v.add(0.0);
                            v.add(index, score);
                            _temp.put(w_n[0], v);
                        }
                        else
                        {
                            Vector<Double> v = new Vector<Double>();
                            for(int i = 0;i<index; i++)
                                v.add(0.0);
                            v.add(index, score);
                            _temp.put(w_n[0], v);
                        }
                    }
                }
                csv.close();
                
                Set<String> temp = _temp.keySet();
                for (Iterator<String> iterator = temp.iterator(); iterator.hasNext();) {
                    String word = (String) iterator.next();
                    Vector<Double> v = _temp.get(word);
                    double score = 0.0;
                    double sum = 0.0;
                    for(int i = 0; i < v.size(); i++)
                        score += ((double)1/(double)(i+1))*v.get(i);
                    for(int i = 1; i<=v.size(); i++)
                        sum += (double)1/(double)i;
                    score /= sum;
                    dict.put(word, score);
                }
            }
            catch(Exception e){e.printStackTrace();}        
        }

public Double extract(String word)
{
    Double total = new Double(0);
    if(dict.get(word+"#n") != null)
         total = dict.get(word+"#n");
    if(dict.get(word+"#a") != null)
        total = dict.get(word+"#a");
    if(dict.get(word+"#r") != null)
        total = dict.get(word+"#r");
    if(dict.get(word+"#v") != null)
        total = dict.get(word+"#v");
    return total;
}



public static class KeyReducer extends Reducer<Text,Text,Text,Text>
{
    private Text result = new Text();
    private static String pathToSWN ="/home/rutesh/workspace/ProductSentiment/SentiWordNet_3.0.0.txt";
    private static HashMap<String, Double> dict;
    
    public static void sentiWord() {

        dict = new HashMap<String, Double>();
        HashMap<String, Vector<Double>> _temp = new HashMap<String, Vector<Double>>();
        try{
            BufferedReader csv =  new BufferedReader(new FileReader(pathToSWN));
            String line = "";           
            while((line = csv.readLine()) != null)
            {
                String[] data = line.split("\t");
                Double score = Double.parseDouble(data[2])-Double.parseDouble(data[3]);
                String[] words = data[4].split(" ");
                for(String w:words)
                {
                    String[] w_n = w.split("#");
                    w_n[0] += "#"+data[0];
                    int index = Integer.parseInt(w_n[1])-1;
                    if(_temp.containsKey(w_n[0]))
                    {
                        Vector<Double> v = _temp.get(w_n[0]);
                        if(index>v.size())
                            for(int i = v.size();i<index; i++)
                                v.add(0.0);
                        v.add(index, score);
                        _temp.put(w_n[0], v);
                    }
                    else
                    {
                        Vector<Double> v = new Vector<Double>();
                        for(int i = 0;i<index; i++)
                            v.add(0.0);
                        v.add(index, score);
                        _temp.put(w_n[0], v);
                    }
                }
            }
            csv.close();
            
            Set<String> temp = _temp.keySet();
            for (Iterator<String> iterator = temp.iterator(); iterator.hasNext();) {
                String word = (String) iterator.next();
                Vector<Double> v = _temp.get(word);
                double score = 0.0;
                double sum = 0.0;
                for(int i = 0; i < v.size(); i++)
                    score += ((double)1/(double)(i+1))*v.get(i);
                for(int i = 1; i<=v.size(); i++)
                    sum += (double)1/(double)i;
                score /= sum;
                dict.put(word, score);
            }
        }
        catch(Exception e){e.printStackTrace();}        
    }

    public static Double extract(String word)
    {
    	Double total = new Double(0);
    	if(dict.get(word+"#n") != null)
    		total = dict.get(word+"#n");
    	if(dict.get(word+"#a") != null)
    		total = dict.get(word+"#a");
    	if(dict.get(word+"#r") != null)
    		total = dict.get(word+"#r");
    	if(dict.get(word+"#v") != null)
    		total = dict.get(word+"#v");
    	return total;
    }

/*public static void main(String[] args) {
//SentiWord test = new SentiWord();
String sentence="hate";
double totalScore = 0;
    if (test.extract(sentence) != null)
    totalScore += test.extract(sentence);
System.out.println(totalScore);
}*/
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
    {
        String translations = "";
        for (Text val : values)
        translations = val.toString();
        double totalScore = 0;
        if (KeyReducer.extract(translations) != null)
        totalScore += KeyReducer.extract(translations);
        String eg =String.valueOf(totalScore);
        //System.out.println(totalScore);
        result.set(eg);
        context.write(key, result);
    }
}

public static void main(String[] args) {
	try {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf,"Task 2");
    job.setJarByClass(MapReduceTwo.class);
    job.setMapperClass(KeyMapper.class);
    job.setReducerClass(KeyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	catch (IOException e) {
		System.out.print(e);
	}
	catch (InterruptedException e) {
		System.out.print(e);
	}
	catch (ClassNotFoundException e) {
		System.out.print(e);
	}
	}

}

public static void main(String[] args) {
    SentiWord test = new SentiWord();
    String sentence="good";
    double totalScore = 0;
        if (test.extract(sentence) != null)
        totalScore += test.extract(sentence);
    System.out.println(totalScore);
}

}