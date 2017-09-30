import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ValueExtractor extends Configured implements Tool {
		
	    public static class KeyMapper4 extends Mapper<Text, Text, Text, Text> {
	        
		   private Text OutputKey = new Text();
		   private Text OutputValue = new Text();
           boolean b = true;
           double totalScore = 0;
           private static String pathToSWN = "/home/rutesh/workspace/Beta/lib/SentiWordNet_3.0.0.txt";
           private static HashMap<String, Double> dict;

           public static void SentiWord() {
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

           public static Double extract(String word) {
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
			
           public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
			
        	   if (b) {
					SentiWord();
					b = false;
				}
				
        	   
				StringTokenizer itr = new StringTokenizer(values.toString());
				boolean tof = false;
		        boolean count=false;
		        while (itr.hasMoreTokens()) {
		        	 
		        	 String[] a = itr.nextToken().split(",");
		        	 if (a[0].equals("RB") || a[0].equals("RBR") || a[0].equals("RBS"))	{
		     			if (a[1].equals("not")){
		     				 tof=true;
	                         count=true; }
		     			 else
	                         if (tof){
				     			tof=false;
				     			count=false;
				     			
									OutputKey.set(key+","+"P");
			                        if (extract(a[1]) != null)
			                        totalScore = extract(a[1]);
									OutputValue.set(Double.valueOf(totalScore).toString());
									context.write(OutputKey, OutputValue);
								}
								else {
								OutputKey.set(key+","+"Q");
			                    if (extract(a[1]) != null)
			                    totalScore = extract(a[1]);
							    OutputValue.set(Double.valueOf(totalScore).toString());
								context.write(OutputKey, OutputValue);
							}
				     		}
	                     
		     		 if (a[0].equals("JJ") || a[0].equals("JJR") || a[0].equals("JJS"))	{
			     		if (tof){
				     			tof=false;
				     			count=false;
				     			OutputKey.set(key+","+"P");
		                        if (extract(a[1]) != null)
		                        totalScore = extract(a[1]);
								OutputValue.set(Double.valueOf(totalScore).toString());
								context.write(OutputKey, OutputValue);
				     		}
			     			else {
			     				OutputKey.set(key+","+"Q");
			                    if (extract(a[1]) != null)
			                    totalScore = extract(a[1]);
							    OutputValue.set(Double.valueOf(totalScore).toString());
								context.write(OutputKey, OutputValue);
			     			}
			     			
		     			
		     		}
		     		
		     		if (!a[0].equals("RB") && !a[0].equals("RBR") && !a[0].equals("RBS") && !a[0].equals("JJ") && !a[0].equals("JJR") && !a[0].equals("JJS") && count==true)
		     			{count=false;tof=false;}
		     		totalScore = 0.0;
		        }
           }
	    }
	    	
	    public static class KeyMapper5 extends Mapper <Text,Text,Text,Text> { 
		   
		   private Text OutputKey = new Text();
		   private Text OutputValue = new Text();
           
           public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
           String a[] = key.toString().split(",");
           
           if(a[2].equals("P")) {
        	   Double i = Double.valueOf(values.toString());
               i = i * -1.0;
               a[2] = "Q";
        	   OutputKey.set(a[0]+","+a[1]+","+a[2]);
        	   OutputValue.set(i.toString());
			   context.write(OutputKey, OutputValue);
             }
           else
           OutputKey.set(key);
           OutputValue.set(values);
		   context.write(OutputKey, OutputValue);
           }
	    }
	   
	    public static class KeyReducer2 extends Reducer<Text,Text,Text,Text> {
	        
	    	private Text result = new Text();
	        
	        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
	        {
	        	Double sum = 0.0;
	            int  counter = 0;
	            for (Text val : values)
	            {
	            Double dval = Double.valueOf(val.toString());
	            if (dval != 0.0) {
	            counter ++;
	            sum += dval;
	            }
	            }
	            Double average = sum/counter;
	            result.set(average.toString());
	            context.write(key, result);
	    }
	    }

	    static Configuration conf;
	    
	    public int run (String[] args) throws Exception {
			
			conf = new Configuration();
			String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		    Job job = Job.getInstance(conf,"Task 1");   
		   
		    //*** FIRST MAPPER CLASS ***//
		    Configuration KeyMapper4Config = new Configuration(false);
		    ChainMapper.addMapper(job, KeyMapper4.class, Text.class, Text.class, Text.class, Text.class, KeyMapper4Config);
		        
		    //*** SECOND MAPPER CLASS ***//
		    Configuration KeyMapper5Config = new Configuration(false);
		    ChainMapper.addMapper(job, KeyMapper5.class, Text.class, Text.class, Text.class, Text.class, KeyMapper5Config);
		        
		    //*** THIRD MAPPER CLASS ***//*
		    /*Configuration KeyMapper3Config = new Configuration(false);
		    ChainMapper.addMapper(job, KeyMapper3.class, Text.class, Text.class, Text.class, Text.class, KeyMapper3Config);
		   */
		    job.setJarByClass(ValueExtractor.class);
		    //job.setMapperClass(KeyMapper1.class);
		    job.setReducerClass(KeyReducer2.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    job.setInputFormatClass(KeyValueTextInputFormat.class);
			
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	        return job.waitForCompletion(true) ? 0 : 1;
		    }
		
		public static void main(String[] args) {
	    	try {
				int res = ToolRunner.run(conf, new ValueExtractor(), args);
				System.exit(res);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
}