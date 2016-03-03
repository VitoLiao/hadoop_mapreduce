package com.schedule.mr.vitality;

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

import com.schedule.mr.time.ScheduleTimeReducer1;
import com.viewlog.util.GbkOutputFormat;
import com.viewlog.util.HdfsOptionUtil;
import com.viewlog.util.ViewlogDateUtil;
import com.viewlog.util.ViewlogOptionUtil;

/**
* @author VitoLiao
* @version 2016年2月29日 上午9:57:29
*/
public class ScheduleVitality {

	public static void main(String[] args) throws IOException, ParseException {
		
		// 获得启动参数
		ViewlogOptionUtil option = new ViewlogOptionUtil(args);
		ArrayList<String> filePathList = new ArrayList<String>();
		
		// 如果输出目录存在，先删除输出目录
		if (HdfsOptionUtil.isPathExist(new String("hdfs://10.80.248.12:8020/vito/output"))) {
			if (HdfsOptionUtil.deletePath(new String("hdfs://10.80.248.12:8020/vito/output"))) {
				System.out.println("Delete output path success.");
			} else {
				System.out.println("Delete output path fail.");
				System.exit(1);
			}
		}
		
		// 获得加载viewlog文件列表
		for (int i = 0; i < option.getDayNum(); i++) {
			Date tmpDate = ViewlogDateUtil.calcWithDayDate(ViewlogDateUtil.str2date(option.getCalcStartDate(), "yyyy-MM-dd"), i);
			String filePath = "hdfs://10.80.248.12:8020/vito/input/Contentviewlog_"
					+ ViewlogDateUtil.date2str(tmpDate, "yyyyMMdd") + ".log";
			System.out.println("filePath : " + filePath);
			filePathList.add(filePath);
		}
		
		Configuration conf01 = new Configuration();
		
		// 将key于value之间默认的分割符转化成指定的符号
		conf01.set("mapred.textoutputformat.separator", ",");
		
		Date startDate = ViewlogDateUtil.str2date(option.getCalcStartDate(), "yyyy-MM-dd");
		
		// schedule读取开始日期，即计算日期的前一天
		Date scheduleStartDate = ViewlogDateUtil.calcWithDayDate(startDate, -1);
		
		// schedule读取结束的日期，即计算天数的后一天
		Date scheduleEndDate = ViewlogDateUtil.calcWithDayDate(startDate, option.getDayNum() + 1);
		
		//将参数传递给map
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
		
		job01.setJarByClass(ScheduleVitality.class);
		
		job01.setMapperClass(ScheduleVitalityMapper1.class);
		job01.setReducerClass(ScheduleVitalityReducer1.class);
		
		job01.setMapOutputKeyClass(Text.class);
		job01.setMapOutputValueClass(Text.class);
		job01.setOutputKeyClass(Text.class);
		job01.setOutputValueClass(IntWritable.class);
		
	
		//配置vielwog路径
		for (int i = 0; i < filePathList.size(); i++) {
			// 获得需要读取的文件路径
			String filePath = filePathList.get(i);

			// 判断路径在hdfs上是否存在
			FileSystem fs = FileSystem.get(URI.create(filePath), conf01);
			Path path = new Path(filePath);
			if (fs.exists(path)) {
				FileInputFormat.addInputPath(job01, new Path("hdfs://10.80.248.12:8020/vito/input/Contentviewlog_*"));
			}
		}
		
//		FileInputFormat.addInputPath(job01, new Path("hdfs://10.80.248.12:8020/vito/input/Contentviewlog_20160215.log"));
		FileInputFormat.addInputPath(job01, new Path("hdfs://10.80.248.12:8020/test/viewlog/viewlog1-11-20160215050010.cdr"));
		FileInputFormat.addInputPath(job01, new Path("hdfs://10.80.248.12:8020/test/viewlog/viewlog1-1-20160215000000.cdr"));
		
		//配置schedule路径
		FileInputFormat.addInputPath(job01, new Path("hdfs://10.80.248.12:8020/vito/input/schedule.dat"));
		
		FileOutputFormat.setOutputPath(job01, new Path("hdfs://10.80.248.12:8020/vito/output/schedule_time_01"));
		
		// 第二次mapreduce
		Configuration conf02 = new Configuration();
		
		// 将key于value之间默认的分割符转化成指定的符号
		conf02.set("mapred.textoutputformat.separator", "|");

		Job job02 = new Job(conf02, "schedule_time_02");
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
