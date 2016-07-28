package com.mr;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
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

import com.util.HdfsOptionUtil;
import com.util.ViewlogDateUtil;
import com.util.ViewlogOptionUtil;

public class LostUser {

	public static void main(String[] args) throws IOException, ParseException {
		int key=0;

		// �����������
		ViewlogOptionUtil option = new ViewlogOptionUtil(args);

		String outputPath = "hdfs://" + option.getHdfsActiveHost() + ":" + option.getPort() + "/utsc/output/lost_user";
		
		ArrayList<String> filePathList = new ArrayList<String>();
		//ArrayList<String> filePathList2 = new ArrayList<String>();

		for (int i = 0; i < 1; i++) {
			Date tmpDate = ViewlogDateUtil.calcWithDayDate(ViewlogDateUtil.str2date(option.getCalcStartDate(), "yyyy-MM-dd"), i);
			String filePath = "hdfs://" + option.getHdfsActiveHost() + ":" + option.getPort() + "/utsc/input_log/Contentviewlog_"
					+ ViewlogDateUtil.date2str(tmpDate, "yyyyMMdd") + ".log";
			System.out.println("filePath1 : " + filePath);
			filePathList.add(filePath);
			//Date date=ChangeStrAndDate.StrToDate(option.getCalcStartDate(), "yyyyMMdd");
//			String filePath2Date=ChangeStrAndDate.MinusDate(option.getCalcStartDate());
			tmpDate = ViewlogDateUtil.calcWithDayDate(tmpDate, -1);
			filePath = "hdfs://" + option.getHdfsActiveHost() + ":" + option.getPort() + "/utsc/input_log/Contentviewlog_"
					+ ViewlogDateUtil.date2str(tmpDate, "yyyyMMdd") + ".log";
			System.out.println("filePath2 : " + filePath);
			filePathList.add(filePath);
			key=filePathList.size();
		}
     if(key==2){
		// ������Ŀ¼���ڣ���ɾ�����Ŀ¼
		if (HdfsOptionUtil.isPathExist(outputPath)) {
			if (HdfsOptionUtil.deletePath(new String(outputPath))) {
				System.out.println("Delete output path success.");
			} else {
				System.out.println("Delete output path fail.");
				System.exit(1);
			}
		}
		//String strdate=option.getCalcStartDate();
		//SimpleDateFormat sp=new SimpleDateFormat("yyyyMMdd");
		String strdate=option.getCalcStartDate();
		// ��һ��mapreduce
		Configuration conf01 = new Configuration();
		
		
		conf01.set("mapred.textoutputformat.separator", "|");
		//���������ݸ�map
		conf01.set("CalcDate", strdate);
		//conf01.set("CalcDate", option.getCalcStartDate());
	//	conf01.set("DateType", option.getCalcType().toString());

		Job job01 = new Job(conf01, "lost_user_01");
		ControlledJob jobCtrl01 = new ControlledJob(conf01);
		jobCtrl01.setJob(job01);
		
		job01.setJarByClass(LostUser.class);

		job01.setMapperClass(LostUserMapper1.class);
		//job01.setCombinerClass(LostUserCombin1.class);
		job01.setReducerClass(LostUserReducer1.class);

		job01.setMapOutputKeyClass(Text.class);
		job01.setMapOutputValueClass(Text.class);
		job01.setOutputKeyClass(Text.class);
		job01.setOutputValueClass(Text.class);
//		job01.setOutputFormatClass(GbkOutputFormat.class);
       job01.setNumReduceTasks(6);
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
		
//		if ((FileInputFormat.getInputPaths(job01)).length == 0) {
//			System.exit(1);
//		}
		//FileInputFormat.addInputPath(job01, new Path("hdfs://10.0.18.98:8020/utsc/input_log/Contentviewlog_20160311.log"));
		//FileInputFormat.addInputPath(job01, new Path("hdfs://10.0.18.98:8020/utsc/input_log/Contentviewlog_20160312.log"));
		FileOutputFormat.setOutputPath(job01, new Path(outputPath + "/lost_user_01"));

		// �ڶ���mapreduce
		Configuration conf02 = new Configuration();

		// ��key��value֮��Ĭ�ϵķָ��ת����ָ���ķ���
		conf02.set("mapred.textoutputformat.separator", "|");
		conf02.set("CalcDate", strdate);
		Job job02 = new Job(conf02, "lost_user_02");
		ControlledJob jobCtrl02 = new ControlledJob(conf02);

		jobCtrl02.setJob(job02);
		jobCtrl02.addDependingJob(jobCtrl01);

		job02.setJarByClass(LostUser.class);

		job02.setMapperClass(LostUserMapper2.class);
		job02.setReducerClass(LostUserReducer2.class);

		job02.setMapOutputKeyClass(Text.class);
		job02.setMapOutputValueClass(Text.class);
		job02.setOutputKeyClass(Text.class);
		job02.setOutputValueClass(Text.class);
//		job02.setOutputFormatClass(GbkOutputFormat.class);
		job02.setNumReduceTasks(6);
		FileInputFormat.addInputPath(job02, new Path(outputPath + "/lost_user_01"));

		FileOutputFormat.setOutputPath(job02, new Path(outputPath + "/lost_user_02"));

		
		
		// ��3��mapreduce
		Configuration conf03= new Configuration();

		// ��key��value֮��Ĭ�ϵķָ��ת����ָ���ķ���
		conf03.set("mapred.textoutputformat.separator", "|");
		conf03.set("CalcDate", strdate);
		Job job03 = new Job(conf03, "lost_user_03");
		ControlledJob jobCtrl03 = new ControlledJob(conf03);

		jobCtrl03.setJob(job03);
		jobCtrl03.addDependingJob(jobCtrl02);

		job03.setJarByClass(LostUser.class);

		job03.setMapperClass(LostUserMapper3.class);
		job03.setCombinerClass(LostUsercombin3.class);
		job03.setReducerClass(LostUserReducer3.class);

		job03.setMapOutputKeyClass(Text.class);
		job03.setMapOutputValueClass(Text.class);
		job03.setOutputKeyClass(Text.class);
		job03.setOutputValueClass(Text.class);
//		job02.setOutputFormatClass(GbkOutputFormat.class);

		FileInputFormat.addInputPath(job03, new Path(outputPath + "/lost_user_02"));

		FileOutputFormat.setOutputPath(job03, new Path(outputPath + "/lost_user_03"));

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
}
