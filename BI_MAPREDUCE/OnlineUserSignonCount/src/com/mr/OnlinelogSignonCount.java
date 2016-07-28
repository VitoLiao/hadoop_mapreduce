package com.mr;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.util.HdfsOptionUtil;
import com.util.ViewlogDateUtil;
import com.util.ViewlogOptionUtil;

public class OnlinelogSignonCount {

	public static void main(String[] args) throws IOException, ParseException {
		
		ViewlogOptionUtil option = new ViewlogOptionUtil(args);
		
		String outputPath = "hdfs://" + option.getHdfsActiveHost() + ":" + option.getPort() + "/utsc/output/online_signon_count_log";
		
		if (HdfsOptionUtil.isPathExist(outputPath)) {
			if (HdfsOptionUtil.deletePath(outputPath)) {
				System.out.println("Delete output path success.");
			} else {
				System.out.println("Delete output path fail.");
				System.exit(1);
			}
		}
		
		Configuration conf01 = new Configuration();
		
		conf01.set("mapred.textoutputformat.separator", "|");
		
		Date calcDay = ViewlogDateUtil.str2date(option.getCalcStartDate(), "yyyy-MM-dd");
		Date calcStartTime = ViewlogDateUtil.getDayStartTime(calcDay);
		Date calcEndTime = ViewlogDateUtil.getDayEndTime(calcDay);
		
		String type = option.getCalcType().toString();
		
		if (type.equalsIgnoreCase("m")) {
			Date monthFirstDate = ViewlogDateUtil.getFirstDayOfMonth(calcDay);
			
			option.setCalcStartDate(ViewlogDateUtil.date2str(monthFirstDate, "yyyy-MM-dd"));
			option.setDayNum(ViewlogDateUtil.getDaysBetween(monthFirstDate, calcDay) + 1);
			
			calcStartTime = ViewlogDateUtil.getDayStartTime(monthFirstDate);
			calcEndTime = ViewlogDateUtil.getDayEndTime(calcDay);
		} else if (type.equalsIgnoreCase("y")) {
			Date yearFirstDate = ViewlogDateUtil.getFirstDayOfYear(calcDay);
			
			option.setCalcStartDate(ViewlogDateUtil.date2str(yearFirstDate, "yyyy-MM-dd"));
			option.setDayNum(ViewlogDateUtil.getDaysBetween(yearFirstDate, calcDay) + 1);
			
			calcStartTime = ViewlogDateUtil.getDayStartTime(yearFirstDate);
			calcEndTime = ViewlogDateUtil.getDayEndTime(calcDay);			 
		}
		
		ArrayList<String> filePathList = option.getOnlinelogPathList();
		
		conf01.set("calcStartTime", ViewlogDateUtil.date2str(calcStartTime) + ".0");
		conf01.set("calcEndTime", ViewlogDateUtil.date2str(calcEndTime) + ".0");
		
		System.out.println("calcStartTime : " + calcStartTime.toString() + ", calcEndTime : " + calcEndTime.toString() + ", daynum : " + option.getDayNum());
		
		Job job01 = new Job(conf01, "online_log_01");
		ControlledJob jobCtrl01 = new ControlledJob(conf01);
		
		jobCtrl01.setJob(job01);
		
		job01.setJarByClass(OnlinelogSignonCount.class);
		
		job01.setMapperClass(OnlinelogMapper1.class);
		job01.setReducerClass(OnlinelogReducer1.class);
		job01.setMapOutputKeyClass(Text.class);
		job01.setMapOutputValueClass(IntWritable.class);
		job01.setOutputKeyClass(Text.class);
		job01.setOutputValueClass(LongWritable.class);
		job01.setNumReduceTasks(48);
		
		for (int i = 0; i < filePathList.size(); i++) {
			if (HdfsOptionUtil.isPathExist(filePathList.get(i))) {
				FileInputFormat.addInputPath(job01, new Path(filePathList.get(i)));
			} else {
				System.out.println("File not exist : " + filePathList.get(i));
			}
		
		}
		
		FileOutputFormat.setOutputPath(job01, new Path(outputPath + "/online_log_01"));
	
		Configuration conf02 = new Configuration();

		conf02.set("DateType", option.getCalcType().toString());
		conf02.set("calcStartTime", ViewlogDateUtil.date2str(calcStartTime) + ".0");
		conf02.set("calcEndTime", ViewlogDateUtil.date2str(calcEndTime) + ".0");

		conf02.set("mapred.textoutputformat.separator", "|");

		Job job02 = new Job(conf02, "online_log_02");
		ControlledJob jobCtrl02 = new ControlledJob(conf02);

		jobCtrl02.setJob(job02);
		jobCtrl02.addDependingJob(jobCtrl01);

		job02.setJarByClass(OnlinelogSignonCount.class);

		job02.setMapperClass(OnlinelogMapper2.class);
		job02.setReducerClass(OnlinelogReducer2.class);

		job02.setMapOutputKeyClass(Text.class);
		job02.setMapOutputValueClass(IntWritable.class);
		job02.setOutputKeyClass(Text.class);
		job02.setOutputValueClass(NullWritable.class);
		job02.setNumReduceTasks(24);

		FileInputFormat.addInputPath(job02, new Path(outputPath + "/online_log_01"));
		
		FileOutputFormat.setOutputPath(job02, new Path(outputPath + "/online_log_02"));		
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
