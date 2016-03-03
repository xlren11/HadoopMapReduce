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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Date;


public class ThetaJoin {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// String[] content = value.toString().split(",");
			// Text k = new Text();
			// Text d1 = new Text();
			// Text d2 = new Text();
			// k.set("a");
			// if (content[3].equals("1")) {
			// 	String data1 = "x," + content[0] + "," + content[1];
			// 	String data2 = "y," + content[0] + "," + content[1];
			// 	d1.set(data1);
			// 	d2.set(data2);
			// 	context.write(k, d1);
			// 	context.write(k, d2);
			// }

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

		public void reduce(Text key, Iterable<Text> values, Context context) 
		throws IOException, InterruptedException{

			// List<String> l1 = new ArrayList<String>();
			// List<String> l2 = new ArrayList<String>();
			// for (Text val: values) {
			// 	String data = val.toString();
			// 	if (data.charAt(0) == 'x') {
			// 		l1.add(data);
			// 	}
			// 	else {
			// 		l2.add(data);
			// 	}
			// }
			// Text result = new Text();
			// for (String s: l1) {
			// 	for (String q: l2) {
			// 		String[] d1 = s.split(",");
			// 		String[] d2 = q.split(",");
			// 		String id1 = d1[2];
			// 		String id2 = d2[2];
			// 		if (!id1.equals(id2)) {
			// 			Date date1 = new Date();
			// 			Date date2 = new Date();
			// 			try {
			// 				date1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(d1[1]);
			// 			}
			// 			catch(ParseException e) {}
			// 			try {
			// 				date2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(d2[1]);
			// 			}
			// 			catch(ParseException e) {}
			// 			if (Math.abs(date1.getTime() - date2.getTime()) / 1000 < 2){
			// 				String res = d1[1] + "," + d1[2] + "," + d2[2] + ",";
			// 				result.set(res);
			// 				context.write(result, new Text());
			// 			}
			// 		}
			// 	}
			// }		

			ArrayList<String> AList = new ArrayList<String>() ;
			ArrayList<String> BList = new ArrayList<String>() ;
			Text nullText = new Text();
			for (Text val: values) {
				String content = val.toString();
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
				Date dateA =  new Date();//new SimpleDateFormat("YYYY-MM-DD HH:mm:ss");
			    Date dateB =  new Date();//SimpleDateFormat("YYYY-MM-DD HH:mm:ss");
			    try{
					dateA = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timeA);
				}
				catch(ParseException pe){
					System.out.println("ERROR: could not parse date in string \"" + timeA + "\"");
				}
				for(int j=0;j<BSize; j++){
					String contentB = BList.get(j);
					String [] numberB = contentB.split(",");
					String timeB = numberB[1];
					String userB = numberB[2];
					if(!userA.equals(userB) ){
						try{
							dateB = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timeB);
						}
						catch(ParseException pe){
							System.out.println("ERROR: could not parse date in string \"" + timeA + "\"");
						}
						//Date dateB = new SimpleDateFormat("YYYY-MM-DD HH:mm:ss").parse(timeB);
						long timeDiff = Math.abs(dateA.getTime() - dateB.getTime())/1000;
						//long timeDiff = Math.abs(timaA - timb);
						if(timeDiff < 2 ){
	//						result += timeA + "," + timeB + "," + queryA +"," + queryB + "\n";
							result += timeA + "," + userA +"," + userB + "\n";
							Text record = new Text(result);
							context.write(record, new Text());
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