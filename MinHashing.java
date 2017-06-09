package org.apache.hadoop.examples;

import java.io.IOException;
import java.io.OutputStream;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class MinHashing{
	
	public static FileSystem fs;
	
	
	public static class MinHashingMap extends Mapper<Text, Text, Text, Text> {
		//@Override
		public void map(Text key, Text value, Context context
                    ) throws IOException, InterruptedException {
			//<K,V> = <(permutationID,documentID), ()>
			String[] data = value.toString().split(",");
			String matrix = data[0];
			
			if (matrix.contains("M")){
				
			}
			if (matrix.contains("P")){
				
			}
		}
	}
	
	public static class MinHashingReduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			HashMap<String, List<Integer>> M = new HashMap<String, List<Integer>>();
			for (Text val : values) {
				String str = val.toString();
				StringBuilder Shingle = new StringBuilder();
				for(int i=0;i<9;i++) Shingle.append(String.valueOf(str.charAt(i)));
				StringBuilder Document = new StringBuilder();
				for(int i=9;i<str.length();i++) Document.append(String.valueOf(str.charAt(i)));
				
				if(M.get(Shingle.toString()) == null){
					M.put(Shingle.toString(),new ArrayList<Integer>());
				}
				List<Integer> temp = M.get(Shingle.toString());
				temp.add(Integer.parseInt(Document.toString()));
				M.put(Shingle.toString(),temp);
			}
			//for every shingle
			for (Object keyItr : M.keySet()){
				List<Integer> temp = M.get(keyItr);
				int[] doc = new int[3];
				//initilize doc
				for(int i=0; i<3; i++){
					doc[i] = 0;
				}
				//traverse the temp array
				for(int i=0; i<temp.size(); i++){
					if(doc[temp.get(i)-1] == 0){
						doc[temp.get(i)-1] = 1;
					}
				}
				String mat = "M,";
				for(int i=0; i<doc.length;i++){
					mat += Integer.toString(doc[i]);
					if(i!=doc.length-1){
						mat += ",";
					}
				}
				
				context.write(null, new Text(mat));
			}
			/*
			for (Text val : values) {
				context.write(new Text(key.toString()),new Text(key.toString()+","+val.toString()));
			}*/
		}
			
	}
	
	public int run() throws Exception {
		Configuration conf = new Configuration();
		//Save params
		conf.set("numOfShingles",Integer.toString(permutation(conf)));
		
		Job job = new Job(conf,"MinHashing");
		
		job.setJarByClass(MinHashing.class);
		job.setMapperClass(MinHashingMap.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(MinHashingReduce.class);
		//mapOutput,reduceOutput
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);  

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPaths(job, "/user/root/data/Matrix.txt,/user/root/data/Permutation.txt");
		
		FileOutputFormat.setOutputPath(job, new Path("/user/root/data/Signature"));

		return (job.waitForCompletion(true) ? 0 : -1);
	}
	
}