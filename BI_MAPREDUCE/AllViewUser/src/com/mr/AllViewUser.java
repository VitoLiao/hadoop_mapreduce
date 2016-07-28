package com.utsc.WL.MR.AllViewUser;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.viewlog.util.HdfsOptionUtil;

public class AllViewUser {

	public static void main(String[] args) throws IOException, ParseException {
		// 获得启动参数
		ArrayList<String> filePathList = new ArrayList<String>();

		// String locationStr = "hdfs://10.80.248.12:8020/CVL/IOCluser";
		// String locationStrIn =
		// "hdfs://10.80.248.12:8020/CVL/IOLocal/WLInput";
		// String locationStrOut =
		// "hdfs://10.80.248.12:8020/CVL/IOLocal/WLOutput";
		// String locationStr = "hdfs://10.80.248.12:8020/CVL/IOLocal";
		// String locationStrIn = "hdfs://10.80.248.12:8020/utsc/input_log";
		// String locationStrOut =
		// "hdfs://10.80.248.12:8020/utsc/output/VLAllViewUser";
		// String locationStrIn = "hdfs://10.0.18.98:8020/utsc/input_log";
		// String locationStrOut =
		// "hdfs://10.0.18.98:8020/utsc/output/VLAllViewUser";

		// String DFlag = "local";
		// String DFlag = "cluster";
		String DFlag = "online";
		// String DFlag = "online1";
		String locationStrIn = "hdfs://10.80.248.12:8020/sampsonTest/input";
		String locationStrOut = "hdfs://10.80.248.12:8020/sampsonTest/output";
		if (DFlag.equals("local")) {
			// String locationStr = "hdfs://10.80.248.12:8020/CVL/IOCluser";
			locationStrIn = "hdfs://10.80.248.12:8020/sampsonTest/input";
			locationStrOut = "hdfs://10.80.248.12:8020/sampsonTest/output";
		} else if (DFlag.equals("cluster")) {
			locationStrIn = "hdfs://10.80.248.12:8020/utsc/input_log";
			locationStrOut = "hdfs://10.80.248.12:8020/utsc/output/VLAllViewUser";
		} else if (DFlag.equals("online")) {
			locationStrIn = "hdfs://10.0.18.98:8020/utsc/input_log";
			locationStrOut = "hdfs://10.0.18.98:8020/utsc/output/VLAllViewUser";
		} else if (DFlag.equals("online1")) {
			locationStrIn = "hdfs://10.0.18.98:8020/rDB/test/input";
			locationStrOut = "hdfs://10.0.18.98:8020/rDB/test/output";
		}

		if (HdfsOptionUtil.isPathExist(new String(locationStrOut))) {
			if (HdfsOptionUtil.deletePath(new String(locationStrOut))) {
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

		String calType = "0";
		String calNum = "0";
		String IPPort = "0";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date now = new Date();
		String startDate = sdf.format(now);
		if (args.length % 2 == 0) {
			for (int i = 0; i < args.length; i++) {
				if (args[i].trim().equals("-s")) {
					startDate = args[i + 1];
				} else if (args[i].trim().equals("-t")) {
					calType = args[i + 1];
				} else if (args[i].trim().equals("-n")) {
					calNum = args[i + 1];
				} else if (args[i].trim().equals("-host")) {
					IPPort = args[i + 1];
				}
				i += 1;
			}
		}

		if (calType.equals("0")) {
			calType = "d";
		}
		if (calNum.equals("0")) {
			calNum = "1";
		}

		if (IPPort.equals("0")) {

		} else {
			locationStrIn = "hdfs://" + IPPort + "/utsc/input_log";
			locationStrOut = "hdfs://" + IPPort + "/utsc/output/VLUserViewArea";
		}
		
		if (HdfsOptionUtil.isPathExist(new String(locationStrOut))) {
			if (HdfsOptionUtil.deletePath(new String(locationStrOut))) {
				System.out.println("Delete output path success.");
			} else {
				System.out.println("Delete output path fail.");
				System.exit(1);
			}
		}
		
		// ArrayList<String> calDate = new ArrayList<String>();
		ArrayList<String> neededDate = new ArrayList<String>();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(sdf.parse(startDate));
		calendar.add(Calendar.DATE, -1);
		neededDate.add(sdf.format(calendar.getTime()));
		calendar.add(Calendar.DATE, 1);
		String calDate = sdf.format(calendar.getTime());
		for (int t = 0; t < Integer.valueOf(calNum); t++) {
			neededDate.add(sdf.format(calendar.getTime()));
			calendar.add(Calendar.DATE, 1);
		}
		neededDate.add(sdf.format(calendar.getTime()));
		// calendar.add(Calendar.DATE, 1);
		// neededDate.add(sdf.format(calendar.getTime()));
		// System.out.println("hh= " + calDate.size());

		for (int i = 0; i < neededDate.size(); i++) {
			String tmpDate = neededDate.get(i).substring(0, 10).replace("-", "");
			String filePath = locationStrIn + "/Contentviewlog_" + tmpDate + ".log";
			System.out.println("filePath : " + filePath);
			filePathList.add(filePath);
		}

		conf01.set("calDate", calDate);
		conf01.set("calType", calType);

		conf01.set("mapred.textoutputformat.separator", "|");

		Job job01 = Job.getInstance(conf01, "AllViewUser_01");
		ControlledJob jobCtrl01 = new ControlledJob(conf01);
		jobCtrl01.setJob(job01);
		job01.setJarByClass(AllViewUser.class);
		job01.setMapperClass(AllViewUserMapper1.class);
		job01.setReducerClass(AllViewUserReducer1.class);
		job01.setMapOutputKeyClass(Text.class);
		job01.setMapOutputValueClass(Text.class);
		job01.setOutputKeyClass(Text.class);
		job01.setOutputValueClass(Text.class);
		// job01.setNumReduceTasks(20);

		for (int i = 0; i < filePathList.size(); i++) {
			String filePath = filePathList.get(i);
			FileSystem fs = FileSystem.get(URI.create(filePath), conf01);
			Path path = new Path(filePath);
			if (fs.exists(path)) {
				FileInputFormat.addInputPath(job01, new Path(filePath));
			}
		}

		// FileInputFormat.addInputPath(job01, new Path(locationStrIn));
		FileOutputFormat.setOutputPath(job01, new Path(locationStrOut + "/wloutput01"));

		// The second MR
		conf02.set("mapred.textoutputformat.separator", "|");
		Job job02 = Job.getInstance(conf02, "AllViewUser_02");
		ControlledJob jobCtrl02 = new ControlledJob(conf02);
		jobCtrl02.setJob(job02);
		jobCtrl02.addDependingJob(jobCtrl01);
		job02.setJarByClass(AllViewUser.class);
		job02.setMapperClass(AllViewUserMapper2.class);
		job02.setReducerClass(AllViewUserReducer2.class);
		job02.setMapOutputKeyClass(Text.class);
		job02.setMapOutputValueClass(Text.class);
		job02.setOutputKeyClass(Text.class);
		job02.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job02, new Path(locationStrOut + "/wloutput01"));
		FileOutputFormat.setOutputPath(job02, new Path(locationStrOut + "/wloutput02"));

		// The Third MR

		conf03.set("mapred.textoutputformat.separator", "|");
		Job job03 = Job.getInstance(conf03, "AllViewUser_03");
		ControlledJob jobCtrl03 = new ControlledJob(conf03);
		jobCtrl03.setJob(job03);
		jobCtrl03.addDependingJob(jobCtrl02);
		job03.setJarByClass(AllViewUser.class);
		job03.setMapperClass(AllViewUserMapper3.class);
		job03.setReducerClass(AllViewUserReducer3.class);
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
