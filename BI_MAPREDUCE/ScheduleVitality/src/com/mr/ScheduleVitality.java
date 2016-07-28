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
public class ScheduleVitality {

	public static void main(String[] args) throws IOException, ParseException {
		
		// �����������
		ViewlogOptionUtil option = new ViewlogOptionUtil(args);
		ArrayList<String> filePathList = option.getViewlogPathList();
		
		String outputPath = "hdfs://" + option.getHdfsActiveHost() + ":" + option.getPort() + "/utsc/output/schedule_vitality";
		
		// ������Ŀ¼���ڣ���ɾ�����Ŀ¼
		if (HdfsOptionUtil.isPathExist(outputPath)) {
			if (HdfsOptionUtil.deletePath(outputPath)) {
				System.out.println("Delete output path success.");
			} else {
				System.out.println("Delete output path fail.");
				System.exit(1);
			}
		}
		
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
		
		System.out.println("scheduleStartDate : " + ViewlogDateUtil.date2str(scheduleStartDate));
		System.out.println("scheduleEndDate : " + ViewlogDateUtil.date2str(scheduleEndDate));
		

		Job job01 = new Job(conf01, "schedule_vitality_01");
		ControlledJob jobCtrl01 = new ControlledJob(conf01);
		
		jobCtrl01.setJob(job01);
		
		job01.setJarByClass(ScheduleVitality.class);
		
		job01.setMapperClass(ScheduleVitalityMapper1.class);
		job01.setReducerClass(ScheduleVitalityReducer1.class);
		
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
		
		FileOutputFormat.setOutputPath(job01, new Path(outputPath + "/schedule_vitality_01"));
	
		// �ڶ���mapreduce
		Configuration conf02 = new Configuration();
		
		// ��key��value֮��Ĭ�ϵķָ��ת����ָ���ķ���
		conf02.set("mapred.textoutputformat.separator", ",");

		Job job02 = new Job(conf02, "schedule_vitality_02");
		ControlledJob jobCtrl02 = new ControlledJob(conf02);

		jobCtrl02.setJob(job02);
		jobCtrl02.addDependingJob(jobCtrl01);

		job02.setJarByClass(ScheduleVitality.class);

		job02.setMapperClass(ScheduleVitalityMapper2.class);
		job02.setReducerClass(ScheduleVitalityReducer2.class);

		job02.setMapOutputKeyClass(Text.class);
		job02.setMapOutputValueClass(IntWritable.class);
		job02.setOutputKeyClass(Text.class);
		job02.setOutputValueClass(IntWritable.class);
		job02.setNumReduceTasks(6);
//		job02.setOutputFormatClass(GbkOutputFormat.class);

		FileInputFormat.addInputPath(job02, new Path(outputPath + "/schedule_vitality_01"));

		FileOutputFormat.setOutputPath(job02, new Path(outputPath + "/schedule_vitality_02"));
		
		// ������mapreduce
		Configuration conf03 = new Configuration();
		
		// ��key��value֮��Ĭ�ϵķָ��ת����ָ���ķ���
		conf03.set("mapred.textoutputformat.separator", "|");

		Job job03 = new Job(conf03, "schedule_vitality_03");
		ControlledJob jobCtrl03 = new ControlledJob(conf03);

		jobCtrl03.setJob(job03);
		jobCtrl03.addDependingJob(jobCtrl02);

		job03.setJarByClass(ScheduleVitality.class);

		job03.setMapperClass(ScheduleVitalityMapper3.class);
		job03.setReducerClass(ScheduleVitalityReducer3.class);

		job03.setMapOutputKeyClass(Text.class);
		job03.setMapOutputValueClass(IntWritable.class);
		job03.setOutputKeyClass(Text.class);
		job03.setOutputValueClass(NullWritable.class);
		job03.setNumReduceTasks(1);
//		job02.setOutputFormatClass(GbkOutputFormat.class);

		FileInputFormat.addInputPath(job03, new Path(outputPath + "/schedule_vitality_02"));

		FileOutputFormat.setOutputPath(job03, new Path(outputPath + "/schedule_vitality_03"));
		
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
