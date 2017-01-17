package com.sample.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("input path ");
		}

		try {
			Job job = Job.getInstance();
			job.setJobName("Word Count");

			// set file input/output path
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			// set jar class name
			job.setJarByClass(WordCount.class);

			// set mapper and reducer to job
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReducer.class);

			// set output key class
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			int returnValue = job.waitForCompletion(true) ? 0 : 1;
			System.out.println(job.isSuccessful());

			System.exit(returnValue);

		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		}
	}
}