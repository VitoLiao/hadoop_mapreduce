package com.mr;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
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

public class ChannelTime {

	public static void main(String[] args) throws IOException, ParseException {

		// �����������
		ViewlogOptionUtil option = new ViewlogOptionUtil(args);

		ArrayList<String> filePathList = new ArrayList<String>();

		filePathList = option.getViewlogPathList();
		
		String outputPath = "hdfs://" + option.getHdfsActiveHost() + ":" + option.getPort() + "/utsc/output/channel_time";

		// ������Ŀ¼���ڣ���ɾ�����Ŀ¼
		if (HdfsOptionUtil.isPathExist(outputPath)) {
			if (HdfsOptionUtil.deletePath(outputPath)) {
				System.out.println("Delete output path success.");
			} else {
				System.out.println("Delete output path fail.");
				System.exit(1);
			}
		}
		
		// ��һ��mapreduce
		Configuration conf01 = new Configuration();
		
		
		conf01.set("mapred.textoutputformat.separator", ",");
		//���������ݸ�map
		conf01.set("CalcDate", option.getCalcStartDate());
		conf01.set("DateType", option.getCalcType().toString());

		Job job01 = new Job(conf01, "channel_time_01");
		ControlledJob jobCtrl01 = new ControlledJob(conf01);
		jobCtrl01.setJob(job01);

		job01.setJarByClass(ChannelTime.class);

		job01.setMapperClass(ChannelTimeMapper1.class);
		job01.setReducerClass(ChannelTimeReducer1.class);

		job01.setMapOutputKeyClass(Text.class);
		job01.setMapOutputValueClass(IntWritable.class);
		job01.setOutputKeyClass(Text.class);
		job01.setOutputValueClass(Text.class);
		job01.setNumReduceTasks(24);
//		job01.setOutputFormatClass(GbkOutputFormat.class);

		for (int i = 0; i < filePathList.size(); i++) {
			// �����Ҫ��ȡ���ļ�·��a
			String filePath = filePathList.get(i);

			// �ж�·����hdfs���Ƿ����
			if (HdfsOptionUtil.isPathExist(filePath)) {
				FileInputFormat.addInputPath(job01, new Path(filePath));
			} else {
				System.out.println("File not exist, skip it : " + filePath);
				continue;
			}
		}
		
//		if ((FileInputFormat.getInputPaths(job01)).length == 0) {
//			System.exit(1);
//		}

		FileOutputFormat.setOutputPath(job01, new Path(outputPath + "/channel_time_01"));

		// �ڶ���mapreduce
		Configuration conf02 = new Configuration();

		// ��key��value֮��Ĭ�ϵķָ��ת����ָ���ķ���
//		conf02.set("mapred.textoutputformat.separator", "|");

		Job job02 = new Job(conf02, "channel_time_02");
		ControlledJob jobCtrl02 = new ControlledJob(conf02);

		jobCtrl02.setJob(job02);
		jobCtrl02.addDependingJob(jobCtrl01);

		job02.setJarByClass(ChannelTime.class);

		job02.setMapperClass(ChannelTimeMapper2.class);
		job02.setReducerClass(ChannelTimeReducer2.class);

		job02.setMapOutputKeyClass(Text.class);
		job02.setMapOutputValueClass(IntWritable.class);
		job02.setOutputKeyClass(Text.class);
		job02.setOutputValueClass(NullWritable.class);
		job02.setNumReduceTasks(8);
//		job02.setOutputFormatClass(GbkOutputFormat.class);

		FileInputFormat.addInputPath(job02, new Path(outputPath + "/channel_time_01"));

		FileOutputFormat.setOutputPath(job02, new Path(outputPath + "/channel_time_02"));

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
