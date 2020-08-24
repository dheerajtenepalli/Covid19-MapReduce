import java.io.*;
import java.util.*;
import java.net.URI;

import org.apache.commons.lang3.text.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.FileSystem; 

public class Covid19_3 {
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		private static DoubleWritable new_cases = new DoubleWritable(0);
		private Text loc = new Text();
		
		// The 4 types declared here should match the types that was declared on the top
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tok = new StringTokenizer(value.toString(), ","); 
			String date_year="";

			try{
				if(tok.hasMoreTokens()) {
					tok.nextToken();
				}
				if(tok.hasMoreTokens()) {
					loc.set(tok.nextToken());
				}
				if(tok.hasMoreTokens()) {
					new_cases.set(Long.parseLong(tok.nextToken()));
				}
				context.write(loc,new_cases);
			}
			catch(Exception e){
				System.out.println("exception occured");
				e.printStackTrace();
			}
		}
		
	}
	
	

	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	// The input types of reduce should match the output type of map
	public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable total_cases_per_million = new DoubleWritable(0);
		Hashtable<String, Long> loc_population = new Hashtable<String,Long>();
		
		public void setup(Context context) throws IOException,  InterruptedException{
			System.out.println("Hello from setup");
			URI[] files_uri = context.getCacheFiles();
			FileSystem fs = FileSystem.get(context.getConfiguration()); 
            Path getFilePath = new Path(files_uri[0].toString());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath))); 

            String line = ""; 
            while ((line = reader.readLine()) != null)  { 
                String[] words = line.split(",");
                try{
                	loc_population.put(words[1],Long.parseLong(words[4]));
                	//System.out.println(" word[1] "+ words[1] + " " + Long.parseLong(words[4]));
            	}
            	catch(Exception e){ 
            		System.out.println("Exception for word[1] "+ words[1]);
            	} 
            }
		}
		// Notice the that 2nd argument: type of the input value is an Iterable collection of objects 
		//  with the same type declared above/as the type of output value from map
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			System.out.println("HELLO INSIDE reduce");
			for (DoubleWritable tmp: values) {
				sum += tmp.get();
			}
			try{
				//System.out.println("key and value"+ key.toString() + " " + loc_population.get(key.toString()));
				long tot_pop = loc_population.get(key.toString());
				double temp = (sum*1000000.0)/tot_pop;
				total_cases_per_million.set(temp);
				context.write(key, total_cases_per_million);

			}catch(Exception e){
				System.out.println("error in key "+ key.toString());
			}
			// This write to the final output
			
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		long st  = new Date().getTime();
		Configuration conf = new Configuration();
		Job myjob = Job.getInstance(conf, "task 3");
		myjob.addCacheFile(new Path(args[1]).toUri());
		myjob.setJarByClass(Covid19_3.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(DoubleWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[2]));
		boolean ret = myjob.waitForCompletion(true);   
		long et = new Date().getTime();
		double est_t = (et - st)/1000.0;
		System.out.println("Time taken for task 3: " + est_t + " sec");
		System.exit(ret ? 0 : 1);
	}
}
