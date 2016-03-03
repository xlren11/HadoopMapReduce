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

public class ThetaJoin {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] content = value.toString().split(",");
			Text k = new Text();
			Text d1 = new Text();
			Text d2 = new Text();
			if (content[3].equals("1")) {
				String data1 = "a," + content[0] + "," + content[1];
				String data2 = "b," + content[0] + "," + content[1];
				k.set("1");
				d1.set(data1);
				d2.set(data2);
				context.write(k, d1);
				context.write(k, d2);
			}
		}
	} 

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) 
		throws IOException, InterruptedException {

			List<String> l1 = new ArrayList<String>();
			List<String> l2 = new ArrayList<String>();
			for (Text val: values) {
				String content = val.toString();
				if (content.charAt(0) == 'b') {
					l1.add(content);
				}
				else {
					l2.add(content);
				}
			}
			Text result = new Text();
			for (int i = 0; i < l1.size(); ++i) {
				String[] data1 = l1.get(i).split(",");
				for (int j = 0; j < l2.size(); ++j) {
					String[] data2 = l2.get(j).split(",");
					String id1 = data1[2];
					String id2 = data2[2];
					if (!id1.equals(id2)) {
						int t1 = Integer.parseInt(data1[1].split(":")[2]);
						int t2 = Integer.parseInt(data2[1].split(":")[2]);
						if (Math.abs(t1 - t2) < 2) {
							String res = data1[1] + data1[2] + data2[2];
							result.set(res);
							context.write(result, new Text());
						}
					}
				}
			}			
		}
	}

	public static void main(String[] args) throws Exception {

		Job job = new Job(new Configuration());

		job.setJarByClass(ThetaJoin.class);


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