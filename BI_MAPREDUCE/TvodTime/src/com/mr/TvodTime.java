package com.mr;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.util.HdfsOptionUtil;
import com.util.ViewlogOptionUtil;

/**
* @author VitoLiao
* @version 2016��3��3�� ����3:42:17
*/
public class TvodTime {

	public static void main(String[] args) throws IllegalArgumentException, IOException, ParseException {


		ViewlogOptionUtil option = new ViewlogOptionUtil(args);

		ArrayList<String> filePathList = option.getViewlogPathList();

		String outputPath = "hdfs://" + option.getHdfsActiveHost() + ":" + option.getPort() + "/utsc/output/tvod_time";
		
		if (HdfsOptionUtil.isPathExist(outputPath)) {
			if (HdfsOptionUtil.deletePath(outputPath)) {
				System.out.println("Delete output path success.");
			} else {
				System.out.println("Delete output path fail.");
				System.exit(1);
			}
		}
		
		Configuration conf01 = new Configuration();
		
		conf01.set("mapred.textoutputformat.separator", ",");
		
		conf01.set("CalcDate", option.getCalcStartDate());
		conf01.set("DateType", option.getCalcType().toString());

		Job job01 = new Job(conf01, "tvod_time_01");
		ControlledJob jobCtrl01 = new ControlledJob(conf01);
		jobCtrl01.setJob(job01);

		job01.setJarByClass(TvodTime.class);

		job01.setMapperClass(TvodTimeMapper1.class);
		job01.setReducerClass(TvodTimeReducer1.class);

		job01.setMapOutputKeyClass(Text.class);
		job01.setMapOutputValueClass(IntWritable.class);
		job01.setOutputKeyClass(Text.class);
		job01.setOutputValueClass(Text.class);
//		job01.setOutputFormatClass(GbkOutputFormat.class);

		for (int i = 0; i < filePathList.size(); i++) {
			String filePath = filePathList.get(i);

			FileSystem fs = FileSystem.get(URI.create(filePath), conf01);
			Path path = new Path(filePath);
			if (fs.exists(path)) {
				FileInputFormat.addInputPath(job01, new Path(filePath));
			}
		}

		FileOutputFormat.setOutputPath(job01, new Path(outputPath + "/tvod_time_01"));

		Configuration conf02 = new Configuration();

		conf02.set("mapred.textoutputformat.separator", "|");

		Job job02 = new Job(conf02, "tvod_time_02");
		ControlledJob jobCtrl02 = new ControlledJob(conf02);

		jobCtrl02.setJob(job02);
		jobCtrl02.addDependingJob(jobCtrl01);

		job02.setJarByClass(TvodTime.class);

		job02.setMapperClass(TvodTimeMapper2.class);
		job02.setReducerClass(TvodTimeReducer2.class);

		job02.setMapOutputKeyClass(Text.class);
		job02.setMapOutputValueClass(IntWritable.class);
		job02.setOutputKeyClass(Text.class);
		job02.setOutputValueClass(NullWritable.class);
		//job02.setOutputFormatClass(GbkOutputFormat.class);

		FileInputFormat.addInputPath(job02, new Path(outputPath + "/tvod_time_01"));

		FileOutputFormat.setOutputPath(job02, new Path(outputPath + "/tvod_time_02"));

		JobControl jobControl = new JobControl("ctr");
		jobControl.addJob(jobCtrl01);
		jobControl.addJob(jobCtrl02);

		Thread t = new Thread(jobControl);

		t.start();

		while (true) {
			if (jobControl.allFinished()) {
				System.out.println(jobControl.getSuccessfulJobList());
				jobControl.stop();
				break;
			}
		}

		System.out.println("Finished");
	
	}

}
