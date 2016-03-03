package com.schedule.mr.time;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.viewlog.util.GbkOutputFormat;
import com.viewlog.util.HdfsOptionUtil;
import com.viewlog.util.ViewlogDateUtil;
import com.viewlog.util.ViewlogOptionUtil;

/**
* @author VitoLiao
* @version 2016��2��29�� ����9:57:29
*/
public class ScheduleTime {

	public static void main(String[] args) throws IOException, ParseException {
		
		// �����������
		ViewlogOptionUtil option = new ViewlogOptionUtil(args);
		ArrayList<String> filePathList = new ArrayList<String>();
		
		// ������Ŀ¼���ڣ���ɾ�����Ŀ¼
		if (HdfsOptionUtil.isPathExist(new String("hdfs://10.80.248.12:8020/vito/output"))) {
			if (HdfsOptionUtil.deletePath(new String("hdfs://10.80.248.12:8020/vito/output"))) {
				System.out.println("Delete output path success.");
			} else {
				System.out.println("Delete output path fail.");
				System.exit(1);
			}
		}
		
		// ��ü���viewlog�ļ��б�
		for (int i = 0; i < option.getDayNum(); i++) {
			Date tmpDate = ViewlogDateUtil.calcWithDayDate(ViewlogDateUtil.str2date(option.getCalcStartDate(), "yyyy-MM-dd"), i);
			String filePath = "hdfs://10.80.248.12:8020/vito/input/Contentviewlog_"
					+ ViewlogDateUtil.date2str(tmpDate, "yyyyMMdd") + ".log";
			System.out.println("filePath : " + filePath);
			filePathList.add(filePath);
		}
		
		Configuration conf01 = new Configuration();
		
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
		conf01.set("ScheduleStartDate", ViewlogDateUtil.date2str(scheduleStartDate, "yyyy-MM-dd"));
		conf01.set("ScheduleEndDate", ViewlogDateUtil.date2str(scheduleEndDate, "yyyy-MM-dd"));
		
		System.out.println("scheduleStartDate : " + ViewlogDateUtil.date2str(scheduleStartDate));
		System.out.println("scheduleEndDate : " + ViewlogDateUtil.date2str(scheduleEndDate));
		

		Job job01 = new Job(conf01, "schedule_time_01");
		ControlledJob jobCtrl01 = new ControlledJob(conf01);
		
		jobCtrl01.setJob(job01);
		
		job01.setJarByClass(ScheduleTime.class);
		
		job01.setMapperClass(ScheduleTimeMapper1.class);
		job01.setReducerClass(ScheduleTimeReducer1.class);
		
		job01.setMapOutputKeyClass(Text.class);
		job01.setMapOutputValueClass(Text.class);
		job01.setOutputKeyClass(Text.class);
		job01.setOutputValueClass(IntWritable.class);
		
	
		//����vielwog·��
		for (int i = 0; i < filePathList.size(); i++) {
			// �����Ҫ��ȡ���ļ�·��
			String filePath = filePathList.get(i);

			// �ж�·����hdfs���Ƿ����
			FileSystem fs = FileSystem.get(URI.create(filePath), conf01);
			Path path = new Path(filePath);
			if (fs.exists(path)) {
				FileInputFormat.addInputPath(job01, new Path("hdfs://10.80.248.12:8020/vito/input/Contentviewlog_*"));
			}
		}
		
//		FileInputFormat.addInputPath(job01, new Path("hdfs://10.80.248.12:8020/vito/input/Contentviewlog_20160215.log"));
//		FileInputFormat.addInputPath(job01, new Path("hdfs://10.80.248.12:8020/test/viewlog/viewlog1-11-20160215050010.cdr"));
		FileInputFormat.addInputPath(job01, new Path("hdfs://10.80.248.12:8020/test/viewlog/viewlog1-1-20160215000000.cdr"));
		
		//����schedule·��
		FileInputFormat.addInputPath(job01, new Path("hdfs://10.80.248.12:8020/vito/input/schedule.dat"));
		
		FileOutputFormat.setOutputPath(job01, new Path("hdfs://10.80.248.12:8020/vito/output/schedule_time_01"));
		
		// �ڶ���mapreduce
		Configuration conf02 = new Configuration();
		
		// ��key��value֮��Ĭ�ϵķָ��ת����ָ���ķ���
		conf02.set("mapred.textoutputformat.separator", "|");

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

		FileInputFormat.addInputPath(job02, new Path("hdfs://10.80.248.12:8020/vito/output/schedule_time_01"));

		FileOutputFormat.setOutputPath(job02, new Path("hdfs://10.80.248.12:8020/vito/output/schedule_time_02"));
		
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
