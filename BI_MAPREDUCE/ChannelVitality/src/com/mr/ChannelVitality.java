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

public class ChannelVitality {

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException, ParseException {

		// �����������
		ViewlogOptionUtil option = new ViewlogOptionUtil(args);

		String outputPath = "hdfs://" + option.getHdfsActiveHost() + ":" + option.getPort() + "/utsc/output/channel_vitality";
		
		// ������Ŀ¼���ڣ���ɾ�����Ŀ¼
		if (HdfsOptionUtil.isPathExist(outputPath)) {
			if (HdfsOptionUtil.deletePath(outputPath)) {
				System.out.println("Delete output path success.");
			} else {
				System.out.println("Delete output path fail.");
				System.exit(1);
			}
		}

		ArrayList<String> filePathList = option.getViewlogPathList();
		
		Configuration conf01 = new Configuration();

		// ��key��value֮��Ĭ�ϵķָ��ת����ָ���ķ���
		conf01.set("mapred.textoutputformat.separator", ",");
		
		// ���������ݸ�map
		conf01.set("CalcDate", option.getCalcStartDate());
		conf01.set("DateType", option.getCalcType().toString());

		Job job01 = new Job(conf01, "channel_vitality_01");
		ControlledJob jobCtrl01 = new ControlledJob(conf01);

		jobCtrl01.setJob(job01);

		job01.setJarByClass(ChannelVitality.class);

		job01.setMapperClass(ChannelVitalityMapper1.class);
		job01.setReducerClass(ChannelVitalityReducer1.class);

		job01.setMapOutputKeyClass(Text.class);
		job01.setMapOutputValueClass(IntWritable.class);
		job01.setOutputKeyClass(Text.class);
		job01.setOutputValueClass(IntWritable.class);

		for (int i = 0; i < filePathList.size(); i++) {
			// �����Ҫ��ȡ���ļ�·��
			String filePath = filePathList.get(i);

			// �ж�·����hdfs���Ƿ����
			FileSystem fs = FileSystem.get(URI.create(filePath), conf01);
			Path path = new Path(filePath);
			if (fs.exists(path)) {
				FileInputFormat.addInputPath(job01, new Path(filePath));
			}
		}

		FileOutputFormat.setOutputPath(job01, new Path(outputPath + "/channel_vitality_01"));

		Configuration conf02 = new Configuration();

		// ��key��value֮��Ĭ�ϵķָ��ת����ָ���ķ���
//		conf02.set("mapred.textoutputformat.separator", "|");

		Job job02 = new Job(conf02, "channel_vitality_02");
		ControlledJob jobCtrl02 = new ControlledJob(conf02);

		jobCtrl02.setJob(job02);
		jobCtrl02.addDependingJob(jobCtrl01);

		job02.setJarByClass(ChannelVitality.class);

		job02.setMapperClass(ChannelVitalityMapper2.class);
		job02.setReducerClass(ChannelVitalityReducer2.class);

		job02.setMapOutputKeyClass(Text.class);
		job02.setMapOutputValueClass(IntWritable.class);
		job02.setOutputKeyClass(Text.class);
		job02.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job02, new Path(outputPath + "/channel_vitality_01"));

		FileOutputFormat.setOutputPath(job02, new Path(outputPath + "/channel_vitality_02"));

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
