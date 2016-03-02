//Standard Java imports
import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
//Hadoop imports
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;

import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
/**
* Tutorial1
*
*/
public class Tutorial1
{
 //The Mapper
	public static class Map extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, IntWritable>
	{
 //Log levels to search for
		private static final Pattern pattern =
		Pattern.compile("(TRACE)|(DEBUG)|(INFO)|(WARN)|(ERROR)|(FATAL)");
		private static final IntWritable accumulator = new
		IntWritable(1);
		private Text logLevel = new Text();
		public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> collector, Reporter reporter)
		throws IOException
		{
 // split on space, '[', and ']'
			final String[] tokens = value.toString().split("[
				\\[\\]]");
			if(tokens != null)
			{
//now find the log level token
				for(final String token : tokens)
				{
					final Matcher matcher =
					pattern.matcher(token);
//log level found
					if(matcher.matches())
					{
						logLevel.set(token);
 //Create the key value pairs
						collector.collect(logLevel,
							accumulator);
					}
				}
			}
		}
	}
 //The Reducer
	public static class Reduce extends MapReduceBase implements
	Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> collector,
			Reporter reporter) throws IOException
		{
			int count = 0;
 //code to aggregate the occurrence
			while(values.hasNext())
			{
				count += values.next().get();
			}
			System.out.println(key + "\t" + count);
			collector.collect(key, new IntWritable(count));
		}
	}
 //The java main method to execute the MapReduce job
	public static void main(String[] args) throws Exception
	{
 //Code to create a new Job specifying the MapReduce class
		final JobConf conf = new JobConf(Tutorial1.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
 // Combiner is commented out – to be used in bonus activity
//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
 //File Input argument passed as a command line argument
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
 //File Output argument passed as a command line argument
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 //statement to execute the job
		JobClient.runJob(conf);
	}
}