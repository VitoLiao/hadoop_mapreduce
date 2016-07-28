package com.mr;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.util.HdfsOptionUtil;
import com.util.ViewlogOptionUtil;



public class PartViewUser {

	public static void main(String[] args) throws IOException, ParseException {
		// 获得启动参数
		ViewlogOptionUtil option = new ViewlogOptionUtil(args);
		ArrayList<String> filePathList = new ArrayList<String>();
		filePathList = option.getViewlogPathList();
//		System.out.println("path size= "+filePathList.size());

//		 String locationStr = "hdfs://10.80.248.12:8020/utsc/input_log";
//		String locationStr = "hdfs://10.80.248.12:8020/CVL/IOLocal";
//		 String locationStrIn = "hdfs://10.80.248.12:8020/utsc/input_log";
//		 String locationStrOut = "hdfs://10.80.248.12:8020/utsc/output/VLPartViewUser";
		
//		 String locationStrIn = "hdfs://10.80.248.12:8020/CVL/IOLocal/WLInput";
//		 String locationStrOut = "hdfs://10.80.248.12:8020/CVL/IOLocal/WLOutput";
		
//		String locationStrIn = "hdfs://10.0.18.98:8020/utsc/input_log";
//		String locationStrOut = "hdfs://" + option.getHdfsActiveHost() + ":" + option.getPort() + "/utsc/output/VLPartViewUser";
		String locationStrOut = "hdfs://" + option.getHdfsActiveHost() + ":" + option.getPort() + "/utsc/output/VLPartViewUser";
//		 System.out.println("option.getDayNum()= "+ option.getDayNum());
//		for (int i = 0; i < option.getDayNum(); i++) {
//			Date tmpDate = ViewlogDateUtil
//					.calcWithDayDate(ViewlogDateUtil.str2date(option.getCalcStartDate(), "yyyyMMdd"), i);
//			String filePath = locationStrIn + "/Contentviewlog_" + ViewlogDateUtil.date2str(tmpDate, "yyyyMMdd")
//					+ ".log";
//			System.out.println("filePath : " + filePath);
//			filePathList.add(filePath);
//		}

		if (HdfsOptionUtil.isPathExist(new String(locationStrOut))) {
			if (HdfsOptionUtil
					.deletePath(new String(locationStrOut))) {
				System.out.println("Delete output path success.");
			} else {
				System.out.println("Delete output path fail.");
				System.exit(1);
			}
		}

		// ***************high light in here**************************

		Configuration conf01 = new Configuration();
		Configuration conf02 = new Configuration();
		Configuration conf03 = new Configuration();
		
		conf01.set("CalcDate", option.getCalcStartDate());
		conf01.set("DateType", option.getCalcType().toString());

		conf01.set("mapred.textoutputformat.separator", "|");

		Job job01 = Job.getInstance(conf01, "PartViewUser_01");
		ControlledJob jobCtrl01 = new ControlledJob(conf01);
		jobCtrl01.setJob(job01);
		job01.setJarByClass(PartViewUser.class);
		job01.setMapperClass(PartViewUserMapper1.class);
		job01.setReducerClass(PartViewUserReducer1.class);
		job01.setMapOutputKeyClass(Text.class);
		job01.setMapOutputValueClass(Text.class);
		job01.setOutputKeyClass(Text.class);
		job01.setOutputValueClass(Text.class);
		job01.setNumReduceTasks(20);

		for (int i = 0; i < filePathList.size(); i++) {
			String filePath = filePathList.get(i);
			if (HdfsOptionUtil.isPathExist(filePath)) {
				FileInputFormat.addInputPath(job01, new Path(filePath));
			} else {
				System.out.println("File not exist, skip it : " + filePath);
				continue;
			}
		}
		
//		for (int i = 0; i < filePathList.size(); i++) {
//			String filePath = filePathList.get(i);
//			FileSystem fs = FileSystem.get(URI.create(filePath), conf01);
//			Path path = new Path(filePath);
//			if (fs.exists(path)) {
//				FileInputFormat.addInputPath(job01, new Path(filePath));
//				System.out.println("pp= "+filePath);
//			}
//		}

//		FileInputFormat.addInputPath(job01, new Path(locationStrIn));
		FileOutputFormat.setOutputPath(job01, new Path(locationStrOut + "/wloutput01"));

		// The second MR
		// Configuration conf02 = new Configuration();
		conf02.set("mapred.textoutputformat.separator", "|");
		Job job02 = Job.getInstance(conf02, "PartViewUser_02");
		ControlledJob jobCtrl02 = new ControlledJob(conf02);
		jobCtrl02.setJob(job02);
		jobCtrl02.addDependingJob(jobCtrl01);
		job02.setJarByClass(PartViewUser.class);
		job02.setMapperClass(PartViewUserMapper2.class);
		job02.setReducerClass(PartViewUserReducer2.class);
		job02.setMapOutputKeyClass(Text.class);
		job02.setMapOutputValueClass(Text.class);
		job02.setOutputKeyClass(Text.class);
		job02.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job02, new Path(locationStrOut + "/wloutput01"));
		FileOutputFormat.setOutputPath(job02, new Path(locationStrOut + "/wloutput02"));

		// The Third MR
		// Configuration conf03 = new Configuration();

		conf03.set("mapred.textoutputformat.separator", "|");
		Job job03 = Job.getInstance(conf03, "PartViewUser_03");
		ControlledJob jobCtrl03 = new ControlledJob(conf03);
		jobCtrl03.setJob(job03);
		jobCtrl03.addDependingJob(jobCtrl02);
		job03.setJarByClass(PartViewUser.class);
		job03.setMapperClass(PartViewUserMapper3.class);
		job03.setReducerClass(PartViewUserReducer3.class);
		job03.setMapOutputKeyClass(Text.class);
		job03.setMapOutputValueClass(Text.class);
		job03.setOutputKeyClass(Text.class);
		job03.setOutputValueClass(Text.class);
		// job03.setNumReduceTasks(100);
		//
		FileInputFormat.addInputPath(job03, new Path(locationStrOut + "/wloutput02"));
		FileOutputFormat.setOutputPath(job03, new Path(locationStrOut + "/wloutput03"));

		JobControl jobControl = new JobControl("ctr");
		jobControl.addJob(jobCtrl01);
		jobControl.addJob(jobCtrl02);
		jobControl.addJob(jobCtrl03);
		// jobControl.addJob(jobCtrl04);
		// jobControl.addJob(jobCtrl05);

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
