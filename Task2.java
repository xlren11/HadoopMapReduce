package org.myorg;

import java.io.IOException;
import java.util.*;
import java.util.Date;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
// Build a 100 times 100 matrix
// There are 10000 reducers, enough for HW4

public class Task2 {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	  	Text region = new Text();
		Text contentA = new Text();
		Text contentB = new Text();
		int dimension = 25;
		String line = value.toString();
		String [] words = line.split(",");
		String clicks = words[3];
		region.set("A");
		if(clicks.equals( "1") ){
			String messagesA = "A," + words[0] + "," + words[1];
			String messagesB = "B," + words[0] + "," + words[1];
			contentA.set(messagesA);
			contentB.set(messagesB);
			context.write(region,contentA);
			context.write(region,contentB);
		}
	  }
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

	  public void reduce(Text key, Iterator<Text> values, Context context) throws IOException, InterruptedException{
	  	ArrayList<String> AList = new ArrayList<String>() ;
		ArrayList<String> BList = new ArrayList<String>() ;
		Text nullText = new Text();
		while (values.hasNext()) {
			String content = values.next().toString();
			if(content.charAt(0) == 'A') AList.add(content);
			else BList.add(content);
		}
		int ASize = AList.size();
		int BSize = BList.size();
		String result = new String();
		for(int i=0;i<ASize; i++){
			String contentA = AList.get(i);
			String [] numberA = contentA.split(",");
			String timeA = numberA[1].split(":")[2];
			String userA = numberA[2];
			for(int j=0;j<BSize; j++){
				String contentB = BList.get(j);
				String [] numberB = contentB.split(",");
				String timeB = numberB[1].split(":")[2];
				String userB = numberB[2];
				if(!userA.equals(userB) ){
					try{
						dateB = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timeB);
					}
					catch(ParseException pe){
						System.out.println("ERROR: could not parse date in string \"" + timeA + "\"");
					}
					Date dateB = new SimpleDateFormat("YYYY-MM-DD HH:mm:ss").parse(timeB);
					long timeDiff = Math.abs(dateA.getTime() - dateB.getTime())/1000;
					long timeDiff = Math.abs(timaA - timb);
					if(timeDiff < 2 ){
//						result += timeA + "," + timeB + "," + queryA +"," + queryB + "\n";
						result += timeA + "," + userA +"," + userB; + "\n";
						Text record = new Text(result);
						context.write(record, new Text());
					}
				}
			}
		}

//		Text record = new Text(result);
//		output.collect(key, record);
	  }
	}

	public static void main(String[] args) throws Exception {
		//long startTime = System.currentTimeMillis();
		Job job = new Job(new Configuration());

		job.setJarByClass(Task2.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

// 	  JobConf conf = new JobConf(HW4.class);
// 	  conf.setJobName("HW4ThetaJoin");

// 	  conf.setOutputKeyClass(Text.class);
// 	  conf.setOutputValueClass(Text.class);

// 	  conf.setMapperClass(Map.class);
// //	  conf.setCombinerClass(Reduce.class);
// 	  conf.setReducerClass(Reduce.class);

// 	  conf.setInputFormat(TextInputFormat.class);
// 	  conf.setOutputFormat(TextOutputFormat.class);

// 	  FileInputFormat.setInputPaths(conf, new Path(args[0]));
// 	  FileOutputFormat.setOutputPath(conf, new Path(args[1]));

// 	  JobClient.runJob(conf);
// 		long endTime   = System.currentTimeMillis();
// 		long totalTime = endTime - startTime;
// 		System.out.println(totalTime/1000.0);
	}
}
