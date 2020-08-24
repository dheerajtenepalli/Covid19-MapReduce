
import java.io.IOException;
import java.util.*;
import java.text.SimpleDateFormat;

import org.apache.commons.lang3.text.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Covid19_2 {
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		private static LongWritable new_deaths = new LongWritable(0);
		private Text loc = new Text();
		private Date start = new Date();
		private Date end = new Date();
		private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);

		public void setup(Context context) throws IOException,  InterruptedException{
			Configuration config = context.getConfiguration();
			try{
				start = formatter.parse(config.get("start").toString());
				end = formatter.parse(config.get("end").toString());
			}catch(Exception e){
				System.out.println("Exception: "  + e);
				System.out.println("Invalid dates");
			}
			
		}
		
		// The 4 types declared here should match the types that was declared on the top
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tok = new StringTokenizer(value.toString(), ","); 
			// Configuration config = context.getConfiguration();
			// String start_date_string = config.get("start");
			// String end_date_string = config.get("end");

			Date current = new Date();

			try{

				if(tok.hasMoreTokens()) {
					current = formatter.parse(tok.nextToken());
				}
				if(tok.hasMoreTokens()) {
					loc.set(tok.nextToken());
				}
				if(tok.hasMoreTokens()) {
					tok.nextToken();
				}
				if(tok.hasMoreTokens()) {
					new_deaths.set(Long.parseLong(tok.nextToken()));
				}

				if(current.compareTo(start)>=0 && current.compareTo(end)<=0)
					context.write(loc,new_deaths);
			}
			catch(Exception e){

			}
			return;
		}
		
	}
	
	

	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	// The input types of reduce should match the output type of map
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable total = new LongWritable();
		
		// Notice the that 2nd argument: type of the input value is an Iterable collection of objects 
		//  with the same type declared above/as the type of output value from map
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
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
		conf.set("start",args[1]);
		conf.set("end",args[2]);

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date start_d = new Date();
		Date end_d = new Date();
		Date min_d = format.parse("2019-12-31");
		Date max_d = format.parse("2020-04-08");

		try{
			start_d = format.parse(args[1]);
			end_d = format.parse(args[2]);

			if(end_d.compareTo(start_d)<0 || start_d.compareTo(min_d)<0 || end_d.compareTo(max_d)>0){
				System.out.println("Invalid dates in input");
				System.exit(1);
			}

		}catch(Exception e){
			System.out.println("Invalid dates in input");
			System.exit(1);
		}

		Job myjob = Job.getInstance(conf, "task 2");
		myjob.setJarByClass(Covid19_2.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[3]));
		boolean ret = myjob.waitForCompletion(true);   
		long et = new Date().getTime();
		double est_t = (et - st)/1000.0;
		System.out.println("Time taken for task 2: " + est_t + " sec");
		System.exit(ret ? 0 : 1);
	}
}
