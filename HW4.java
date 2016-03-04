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
//import org.apache.hadoop.mapred.*;
//import org.apache.hadoop.util.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
// Build a 100 times 100 matrix
// There are 10000 reducers, enough for HW4

public class HW4 {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
//	  private Text region = new Text();
//	  private Text contentA = new Text();
//	  private Text contentB = new Text();
//	  private int dimension = 25;
//	  Random generater = new Random();
	  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {		
	  //public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	 //  	Text region = new Text();
		// Text contentA = new Text();
		// Text contentB = new Text();
		//int dimension = 25;
		//String line = value.toString();
		String [] words = value.toString().split(",");
		//String clicks = words[3];
	  	//Random generater = new Random();
		if(words[3].equals( "1") && !words[1].isEmpty()){
			String dataA = "A," + words[0] + "," + words[1];
			String dataB = "B," + words[0] + "," + words[1];
			context.write(new Text("AAA"), new Text(dataA));
			context.write(new Text("AAA"), new Text(dataB));
		}
	  }
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
//	  private ArrayList<String> AList = new ArrayList<String>() ;
//	  private ArrayList<String> BList = new ArrayList<String>() ;
//	  @override
	  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	  	ArrayList<String> listA = new ArrayList<String>() ;
		ArrayList<String> listB = new ArrayList<String>() ;
		//Text nullText = new Text();
		// for (Text val: values) {
		// 	String[] ss = val.toString().split(",");
		// 	context.write(nullText,new Text(ss[2]));
		// }
		//while (values.hasNext()) {
		for (Text val: values) {
			String content = val.toString();
			//String content = values.next().toString();
			if(content.charAt(0) == 'A') listA.add(content);
			else listB.add(content);
		}
		//int ASize = AList.size();
		//int BSize = BList.size();
		//String result = new String();
		//for(int i=0;i<ASize; i++){
		for (String strA: listA) {
			for (String strB: listB) {
			//String contentA = AList.get(i);
				String [] dataA = strA.split(",");
				String [] dataB = strB.split(",");
				// String ymdhA = dataA[1];
				// String timeB = dataB[1];
				String idA = dataA[2];
				String idB = dataB[2];
				if(!idA.equals(idB)){
				//String queryA = numberA[3];
					Date dateA =  new Date();//new SimpleDateFormat("YYYY-MM-DD HH:mm:ss");
					Date dateB =  new Date();//SimpleDateFormat("YYYY-MM-DD HH:mm:ss");
					try{
						dateA = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dataA[1]);
					}
					catch(ParseException pe){
						//System.out.println("ERROR: could not parse date in string \"" + timeA + "\"");
					}
			//for(int j=0;j<BSize; j++){
			//for (String strB: listB) {
				//String contentB = BList.get(j);

				//String queryB = numberB[3];
				
					try{
						dateB = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dataB[1]);
					}
					catch(ParseException pe){
						//System.out.println("ERROR: could not parse date in string \"" + timeA + "\"");
					}
//					Date dateB = new SimpleDateFormat("YYYY-MM-DD HH:mm:ss").parse(timeB);
					long diff = Math.abs(dateA.getTime() - dateB.getTime()) / 1000;
					if (diff < 2 ){
//						result += timeA + "," + timeB + "," + queryA +"," + queryB + "\n";
						String result = dataA[1] + "," + idA +"," + idB;
						//result = timeA + "," + queryA +"," + queryB;
						//Text record = new Text(result);
						//output.collect(nullText,record);

						context.write(new Text(), new Text(result));
					}
				}
			}
		}

//		Text record = new Text(result);
//		output.collect(key, record);
	  }
	}

	public static void main(String[] args) throws Exception {
		Job job = new Job(new Configuration());

		job.setJarByClass(HW4.class);


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