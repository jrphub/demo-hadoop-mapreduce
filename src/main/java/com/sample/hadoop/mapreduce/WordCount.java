package com.sample.hadoop.mapreduce;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static void main(String[] args) {
		//Add argument in run configuration: sample.txt output
		//delete output directory if any
		if (args.length < 2) {
			System.err.println("input path ");
		}

		try {
			Job job = Job.getInstance();
			job.setJobName("Word Count");

			// set file input/output path
			FileInputFormat.addInputPath(job, new Path("input-dir/sample.txt"));
			FileOutputFormat.setOutputPath(job, new Path("output/mapreduce_output"));
			
			try {
				FileUtils
						.deleteDirectory(new File(
								"output/mapreduce_output"));
			} catch (IOException e) {
				e.printStackTrace();
			}

			// set jar class name
			job.setJarByClass(WordCount.class);

			// set mapper and reducer to job
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReducer.class);

			// set output key class
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			int returnValue = job.waitForCompletion(true) ? 0 : 1;
			System.out.println("isSuccessful : " + job.isSuccessful());

			System.exit(returnValue);

		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			e.printStackTrace();
		}
	}
}