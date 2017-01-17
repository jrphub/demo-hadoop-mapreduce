package com.sample.hadoop.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// getting the string token from the value.
		StringTokenizer strTokens = new StringTokenizer(value.toString());

		// iterating the strTokens for
		while (strTokens.hasMoreTokens()) {

			// the word present inside the input file
			String word = strTokens.nextToken();

			// writing the word into context with occurrence as 1
			// example: apple 1, mango 1, apple 1
			context.write(new Text(word), new IntWritable(1));
		}

	}

	@Override
	protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("calls only once at startup");
	}

	@Override
	protected void cleanup(Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("calls only once at end");
	}
}