package com.utsc.RS.MR.ProgramRecUseCosine;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProRecUseCosine {

	public static void main(String[] args) throws IOException, ParseException {

		// readme: first setup the input parameter : 10 10

		ArrayList<String> filePathList = new ArrayList<String>();
		//
		// String locationStr = "hdfs://10.0.18.98:8020/rDBAllData";
		//
//		String DFlag = "local";
//		 String DFlag = "clusters";
		 String DFlag = "online";
		String locationStr = "hdfs://10.80.248.12:8020/recommandationDB";
		String filePath = locationStr + "/RSInput/ws_mergedmedia_image";
		// String filePath = locationStr+"/RSInput/ws_mergedmedia_image_online";
		if (DFlag.equals("local")) {
			locationStr = "hdfs://10.80.248.12:8020/recommandationDB";
			filePath = locationStr + "/RSInput/ws_mergedmedia_image";
		} else if (DFlag.equals("clusters")) {
			locationStr = "hdfs://10.80.248.12:8020/rDB";
			filePath = locationStr + "/RSInput/ws_mergedmedia_image";
		} else if (DFlag.equals("online")) {
			locationStr = "hdfs://10.0.18.98:8020/rDB";
			filePath = locationStr + "/RSInput/ws_mergedmedia_image";
		}

		String IPPort = "0";
		if (args.length > 4) {
			if (args[3].trim().equals("-host")) {
				IPPort = args[4];
			}
		}

		if (IPPort.equals("0")) {

		} else {
			locationStr = "hdfs://" + IPPort + "/rDB";
			filePath = locationStr + "/RSInput/ws_mergedmedia_image";
		}

		System.out.println("filePath : " + filePath);
		filePathList.add(filePath);

		if (isPathExist(new String(locationStr + "/RSOutput"))) {
			if (deletePath(new String(locationStr + "/RSOutput"))) {
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
		Configuration conf04 = new Configuration();
		Configuration conf05 = new Configuration();

		conf02.set("rownum", args[0]); // default value:10; row number in
										// reducer2
		conf03.set("hLen", args[1]);
		conf04.set("pLen", args[2]); // default value:10; program number for
										// each program in mapper3

		conf01.set("mapred.textoutputformat.separator", "|");

		Job job01 = Job.getInstance(conf01, "ProRecUseCosine_01");
		ControlledJob jobCtrl01 = new ControlledJob(conf01);
		jobCtrl01.setJob(job01);
		job01.setJarByClass(ProRecUseCosine.class);
		job01.setMapperClass(ProRecUseCosineMapper1.class);
		job01.setReducerClass(ProRecUseCosineReducer1.class);
		job01.setMapOutputKeyClass(Text.class);
		job01.setMapOutputValueClass(Text.class);
		job01.setOutputKeyClass(Text.class);
		job01.setOutputValueClass(Text.class);
		// job01.setNumReduceTasks(10);

		for (int i = 0; i < filePathList.size(); i++) {
			filePath = filePathList.get(i);
			FileSystem fs = FileSystem.get(URI.create(filePath), conf01);
			Path path = new Path(filePath);
			if (fs.exists(path)) {
				FileInputFormat.addInputPath(job01, new Path(filePath));
			}
		}

		// FileInputFormat.addInputPath(job01, new
		// Path(locationStr+"/RSInput"));
		FileOutputFormat.setOutputPath(job01, new Path(locationStr + "/RSOutput/rsoutput01"));

		// The second MR
		// Configuration conf02 = new Configuration();
		conf02.set("mapred.textoutputformat.separator", "###");
		Job job02 = Job.getInstance(conf02, "ProRecUseCosine_02");
		ControlledJob jobCtrl02 = new ControlledJob(conf02);
		jobCtrl02.setJob(job02);
		jobCtrl02.addDependingJob(jobCtrl01);
		job02.setJarByClass(ProRecUseCosine.class);
		job02.setMapperClass(ProRecUseCosineMapper2.class);
		job02.setReducerClass(ProRecUseCosineReducer2.class);
		job02.setMapOutputKeyClass(Text.class);
		job02.setMapOutputValueClass(Text.class);
		job02.setOutputKeyClass(Text.class);
		job02.setOutputValueClass(Text.class);
		job02.setNumReduceTasks(36);

		FileInputFormat.addInputPath(job02, new Path(locationStr + "/RSOutput/rsoutput01"));
		FileOutputFormat.setOutputPath(job02, new Path(locationStr + "/RSOutput/rsoutput02"));

		// The Third MR
		// Configuration conf03 = new Configuration();

		conf03.set("mapred.textoutputformat.separator", "###");
		Job job03 = Job.getInstance(conf03, "ProRecUseCosine_03");
		ControlledJob jobCtrl03 = new ControlledJob(conf03);
		jobCtrl03.setJob(job03);
		jobCtrl03.addDependingJob(jobCtrl02);
		job03.setJarByClass(ProRecUseCosine.class);
		job03.setMapperClass(ProRecUseCosineMapper3.class);
		job03.setReducerClass(ProRecUseCosineReducer3.class);
		job03.setMapOutputKeyClass(Text.class);
		job03.setMapOutputValueClass(Text.class);
		job03.setOutputKeyClass(Text.class);
		job03.setOutputValueClass(Text.class);
		job03.setNumReduceTasks(36);

		FileInputFormat.addInputPath(job03, new Path(locationStr + "/RSOutput/rsoutput02"));
		FileOutputFormat.setOutputPath(job03, new Path(locationStr + "/RSOutput/rsoutput03"));
		//
		// The Four MR
		// Configuration conf04 = new Configuration();
		//
		conf04.set("mapred.textoutputformat.separator", "###");

		Job job04 = Job.getInstance(conf04, "ProRecUseCosine_04");
		ControlledJob jobCtrl04 = new ControlledJob(conf04);

		// TextInputFormat.NUM_INPUT_FILES = 300;
		jobCtrl04.setJob(job04);
		jobCtrl04.addDependingJob(jobCtrl03);
		job04.setJarByClass(ProRecUseCosine.class);
		job04.setMapperClass(ProRecUseCosineMapper4.class);
		job04.setReducerClass(ProRecUseCosineReducer4.class);
		job04.setMapOutputKeyClass(Text.class);
		job04.setMapOutputValueClass(Text.class);
		job04.setOutputKeyClass(Text.class);
		job04.setOutputValueClass(Text.class);
		job04.setNumReduceTasks(9);
		FileInputFormat.addInputPath(job04, new Path(locationStr + "/RSOutput/rsoutput03"));
		FileOutputFormat.setOutputPath(job04, new Path(locationStr + "/RSOutput/rsoutput04"));

		conf05.set("mapred.textoutputformat.separator", "|");

		Job job05 = Job.getInstance(conf05, "ProRecUseCosine_05");
		ControlledJob jobCtrl05 = new ControlledJob(conf05);

		jobCtrl05.setJob(job05);
		jobCtrl05.addDependingJob(jobCtrl04);
		job05.setJarByClass(ProRecUseCosine.class);
		job05.setMapperClass(ProRecUseCosineMapper5.class);
		job05.setReducerClass(ProRecUseCosineReducer5.class);
		job05.setMapOutputKeyClass(Text.class);
		job05.setMapOutputValueClass(Text.class);
		job05.setOutputKeyClass(Text.class);
		// job05.setOutputValueClass(GbkOutputFormat.class);

		FileInputFormat.addInputPath(job05, new Path(locationStr + "/RSOutput/rsoutput04"));
		FileOutputFormat.setOutputPath(job05, new Path(locationStr + "/RSOutput/rsoutput05"));

		JobControl jobControl = new JobControl("ctr");
		jobControl.addJob(jobCtrl01);
		jobControl.addJob(jobCtrl02);
		jobControl.addJob(jobCtrl03);
		jobControl.addJob(jobCtrl04);
		jobControl.addJob(jobCtrl05);

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

	// hdfs folders operation
	public static boolean deletePath(String path) throws IOException {
		boolean isDeleted = false;

		Configuration conf = new Configuration();
		FileSystem deletFs = FileSystem.get(URI.create(path), conf);
		Path delef = new Path(path);

		if (deletFs.delete(delef, true)) {
			isDeleted = true;
		}

		return isDeleted;
	}

	// 锟叫讹拷路锟斤拷锟斤拷hdfs锟斤拷锟角凤拷锟斤拷锟�
	public static boolean isPathExist(String filePath) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(filePath), conf);
		Path path = new Path(filePath);
		if (fs.exists(path)) {
			return true;
		}
		return false;
	}

}
