import java.io.IOException;
import java.util.*;

import org.apache.commons.lang3.text.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Covid19_1 {
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		private static LongWritable new_cases = new LongWritable(0);
		private Text loc = new Text();
		private String include_world;

		public void setup(Context context) throws IOException,  InterruptedException{
			Configuration conf = context.getConfiguration();
			include_world = conf.get("is_world");
		}
		
		// The 4 types declared here should match the types that was declared on the top
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tok = new StringTokenizer(value.toString(), ","); 
			String date_year="";
			// Configuration config = context.getConfiguration();
			// String include_world = config.get("is_world");

			try{
				if(tok.hasMoreTokens()) {
					date_year = tok.nextToken().split("-")[0];
				}
				if(tok.hasMoreTokens()) {
					loc.set(tok.nextToken());
				}
				if(tok.hasMoreTokens()) {
					new_cases.set(Long.parseLong(tok.nextToken()));
				}

				if(!date_year.equals("2020"))
					return;
				if(include_world.equals("false") && (loc.toString().contains("World") || loc.toString().contains("International")))
					return;
				context.write(loc,new_cases);
			}
			catch(Exception e){

			}
		}
		
	}
	
	

	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	// The input types of reduce should match the output type of map
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable total = new LongWritable();
		
		// public void setup(Context context) throws IOException,  InterruptedException{
		// 	System.out.println("HELLO from setup");
		// }
		// Notice the that 2nd argument: type of the input value is an Iterable collection of objects 
		//  with the same type declared above/as the type of output value from map
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			// System.out.println("HELLO");
			long sum = 0;
			for (LongWritable tmp: values) {
				sum += tmp.get();
			}
			total.set(sum);
			// This write to the final output
			context.write(key, total);
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		long st  = new Date().getTime();
		Configuration conf = new Configuration();
		conf.set("is_world",args[1]);
		Job myjob = Job.getInstance(conf, "my word count test");
		myjob.setJarByClass(Covid19_1.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[2]));
		boolean ret = myjob.waitForCompletion(true);   
		long et = new Date().getTime();
		double est_t = (et - st)/1000.0;
		System.out.println("Time taken for task 1: " + est_t + " sec");
		System.exit(ret ? 0 : 1);
	}
}
