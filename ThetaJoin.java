package org.myorg;

import java.io.IOException;
import java.util.*;
import java.util.Date;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;

public class ThetaJoin {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {		
			String [] words = value.toString().split(",");
			if(words[3].equals( "1") && !words[1].isEmpty()){
				String data = words[0] + "," + words[1];
				context.write(new Text("AAA"), new Text(data));
			}
	  	}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	  		ArrayList<String> list = new ArrayList<String>() ;
			for (Text val: values) {
				list.add(val.toString());
			}
			Set<String> set = new HashSet<String>();
			for (int i = 0; i < list.size(); ++i) {
				for (int j = 0; j < list.size(); ++j) {
					if (i == j) continue;
					String [] dataA = list.get(i).split(",");
					String [] dataB = list.get(j).split(",");
					String idA = dataA[1];
					String idB = dataB[1];
					if (!idA.equals(idB)){
						Date dateA =  new Date();
						Date dateB =  new Date();
						try {
							dateA = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dataA[0]);
						}
						catch(ParseException pe){}
						try{ 
							dateB = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dataB[0]);
						}
						catch(ParseException pe){}
						long diff = Math.abs(dateA.getTime() - dateB.getTime()) / 1000;
						if (diff < 2 ){
							String result1 = dataA[0] + "," + idA +"," + idB;
							String result2 = dataB[0] + "," + idB +"," + idA;
							if (!set.contains(result2)) {
								set.add(result1);
								context.write(new Text(), new Text(result1));
							}
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