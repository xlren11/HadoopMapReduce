package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Counting {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text collection = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] content = value.toString().split(",");
			String info = content[2] + "," + content[3] + "," + content[4];
			word.set(content[5]);
			collection.set(info);
			context.write(word, collection);
		}
	} 

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) 
		throws IOException, InterruptedException {
			int cnt1 = 0, cnt2 = 0, cnt3 = 0;
			for (Text val: values) {
				String[] content = val.toString().split(",");
                if (content[0].equals("1")) ++cnt1;
                if (content[1].equals("1")) ++cnt2;
                if (content[2].equals("1")) ++cnt3;
			}
			String info = key.toString() + "," + cnt1 + "," + cnt2 + "," + cnt3;

			context.write(new Text(info), new Text());
		}
	}

	public static void main(String[] args) throws Exception {

		Job job = new Job(new Configuration());

		job.setJarByClass(Counting.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}