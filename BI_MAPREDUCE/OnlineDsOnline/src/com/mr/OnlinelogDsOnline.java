package com.mr;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class OnlinelogDsOnline {

	public static void main(String[] args) throws IOException, ParseException {
		
		ViewlogOptionUtil option = new ViewlogOptionUtil(args);
		
		String outputPath = "hdfs://" + option.getHdfsActiveHost() + ":" + option.getPort() + "/utsc/output/online_ds_online_log";
		
		if (HdfsOptionUtil.isPathExist(outputPath)) {
			if (HdfsOptionUtil.deletePath(outputPath)) {
				System.out.println("Delete output path success.");
			} else {
				System.out.println("Delete output path fail.");
				System.exit(1);
			}
		}
		
		ArrayList<String> filePathList = option.getOnlinelogPathList();
		
		Configuration conf01 = new Configuration();
		
		conf01.set("mapred.textoutputformat.separator", "|");
		
		Date calcStartTime = ViewlogDateUtil.getDayStartTime(ViewlogDateUtil.str2date(option.getCalcStartDate(), "yyyy-MM-dd"));
		Date calcEndTime = ViewlogDateUtil.getDayEndTime(ViewlogDateUtil.str2date(option.getCalcStartDate(), "yyyy-MM-dd"));
		
		conf01.set("calcStartTime", ViewlogDateUtil.date2str(calcStartTime) + ".0");
		conf01.set("calcEndTime", ViewlogDateUtil.date2str(calcEndTime) + ".0");
		
		Job job01 = new Job(conf01, "online_ds_online_01");
		ControlledJob jobCtrl01 = new ControlledJob(conf01);
		
		jobCtrl01.setJob(job01);
		
		job01.setJarByClass(OnlinelogDsOnline.class);
		
		job01.setMapperClass(OnlinelogMapper1.class);
		job01.setReducerClass(OnlinelogReducer1.class);
		job01.setMapOutputKeyClass(Text.class);
		job01.setMapOutputValueClass(Text.class);
		job01.setOutputKeyClass(Text.class);
		job01.setOutputValueClass(NullWritable.class);
		job01.setNumReduceTasks(1);
		
		for (int i = 0; i < filePathList.size(); i++) {
			String filePath = filePathList.get(i);

			FileSystem fs = FileSystem.get(URI.create(filePath), conf01);
			Path path = new Path(filePath);
			if (fs.exists(path)) {
				FileInputFormat.addInputPath(job01, new Path(filePath));
			}
		}
		
		FileOutputFormat.setOutputPath(job01, new Path(outputPath + "/online_log_01"));
	
		JobControl jobControl = new JobControl("ctr");
		jobControl.addJob(jobCtrl01);
		
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
