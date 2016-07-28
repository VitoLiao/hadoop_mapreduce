package com.mr;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

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
import com.util.ViewlogDateUtil;
import com.util.ViewlogOptionUtil;

/**
* @author VitoLiao
* @version 2016��2��29�� ����9:57:29
*/
public class ScheduleTime {

	public static void main(String[] args) throws IOException, ParseException {
		
		// �����������
		ViewlogOptionUtil option = new ViewlogOptionUtil(args);
		
		String outputPath = "hdfs://" + option.getHdfsActiveHost() + ":" + option.getPort() + "/utsc/output/schedule_time";
		
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
		
//		// ��ü���viewlog�ļ��б�
//		for (int i = 0; i < option.getDayNum(); i++) {
//			Date tmpDate = ViewlogDateUtil.calcWithDayDate(ViewlogDateUtil.str2date(option.getCalcStartDate(), "yyyy-MM-dd"), i);
//			String filePath = option.getViewlogPath()
//					+ ViewlogDateUtil.date2str(tmpDate, "yyyyMMdd") + ".log";
//			System.out.println("filePath : " + filePath);
//			filePathList.add(filePath);
//		}
		
		Configuration conf01 = new Configuration();
		
		// ��key��value֮��Ĭ�ϵķָ��ת����ָ���ķ���
		conf01.set("mapred.textoutputformat.separator", ",");
		
		Date startDate = ViewlogDateUtil.str2date(option.getCalcStartDate(), "yyyy-MM-dd");
		
		// schedule��ȡ��ʼ���ڣ����������ڵ�ǰһ��
		Date scheduleStartDate = ViewlogDateUtil.calcWithDayDate(startDate, -1);
		
		// schedule��ȡ���������ڣ������������ĺ�һ��
		Date scheduleEndDate = ViewlogDateUtil.calcWithDayDate(startDate, option.getDayNum() + 1);
		
		//���������ݸ�map
		conf01.set("CalcDate", option.getCalcStartDate());
		conf01.set("DateType", option.getCalcType().toString());
		conf01.set("DayNum", String.valueOf(option.getDayNum()));
		conf01.set("ScheduleStartDate", ViewlogDateUtil.date2str(scheduleStartDate, "yyyyMMdd"));
		conf01.set("ScheduleEndDate", ViewlogDateUtil.date2str(scheduleEndDate, "yyyyMMdd"));
		
		System.out.println("option.getCalcStartDate() : " + option.getCalcStartDate());
		System.out.println("scheduleStartDate : " + ViewlogDateUtil.date2str(scheduleStartDate));
		System.out.println("scheduleEndDate : " + ViewlogDateUtil.date2str(scheduleEndDate));
		

		Job job01 = new Job(conf01, "schedule_time_01");
		ControlledJob jobCtrl01 = new ControlledJob(conf01);
		
		jobCtrl01.setJob(job01);
		
		job01.setJarByClass(ScheduleTime.class);
		
		job01.setMapperClass(ScheduleTimeMapper1.class);
		//job01.setCombinerClass(ScheduleCombin.class);
		job01.setReducerClass(ScheduleTimeReducer1.class);
		job01.setMapOutputKeyClass(Text.class);
		job01.setMapOutputValueClass(Text.class);
		job01.setOutputKeyClass(Text.class);
		job01.setOutputValueClass(IntWritable.class);
		job01.setNumReduceTasks(24);
		//����vielwog·��
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
		
		//����schedule·��
		FileInputFormat.addInputPath(job01, new Path("hdfs://" + option.getHdfsActiveHost() + ":" + option.getPort() + "/utsc/input_table/schedule/part-m-00000"));
		
		FileOutputFormat.setOutputPath(job01, new Path(outputPath + "/schedule_time_01"));
		
//		try {
//			System.exit(job01.waitForCompletion(true) ? 0 : 1);
//		} catch (ClassNotFoundException | InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	
		// �ڶ���mapreduce
		Configuration conf02 = new Configuration();
		
		// ��key��value֮��Ĭ�ϵķָ��ת����ָ���ķ���
		conf02.set("mapred.textoutputformat.separator", ",");

		Job job02 = new Job(conf02, "schedule_time_02");
		ControlledJob jobCtrl02 = new ControlledJob(conf02);

		jobCtrl02.setJob(job02);
		jobCtrl02.addDependingJob(jobCtrl01);

		job02.setJarByClass(ScheduleTime.class);

		job02.setMapperClass(ScheduleTimeMapper2.class);
		job02.setReducerClass(ScheduleTimeReducer2.class);

		job02.setMapOutputKeyClass(Text.class);
		job02.setMapOutputValueClass(IntWritable.class);
		job02.setOutputKeyClass(Text.class);
		job02.setOutputValueClass(IntWritable.class);
//		job02.setOutputFormatClass(GbkOutputFormat.class);
      job02.setNumReduceTasks(6);
		FileInputFormat.addInputPath(job02, new Path(outputPath + "/schedule_time_01"));

		FileOutputFormat.setOutputPath(job02, new Path(outputPath + "/schedule_time_02"));
		
		// ������mapreduce
		Configuration conf03 = new Configuration();
		
		// ��key��value֮��Ĭ�ϵķָ��ת����ָ���ķ���
		conf03.set("mapred.textoutputformat.separator", "|");

		Job job03 = new Job(conf03, "schedule_time_03");
		ControlledJob jobCtrl03 = new ControlledJob(conf03);

		jobCtrl03.setJob(job03);
		jobCtrl03.addDependingJob(jobCtrl02);

		job03.setJarByClass(ScheduleTime.class);

		job03.setMapperClass(ScheduleTimeMapper3.class);
		job03.setReducerClass(ScheduleTimeReducer3.class);

		job03.setMapOutputKeyClass(Text.class);
		job03.setMapOutputValueClass(IntWritable.class);
		job03.setOutputKeyClass(Text.class);
		job03.setOutputValueClass(NullWritable.class);
//		job02.setOutputFormatClass(GbkOutputFormat.class);
		job03.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job03, new Path(outputPath + "/schedule_time_02"));

		FileOutputFormat.setOutputPath(job03, new Path(outputPath + "/schedule_time_03"));		
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
