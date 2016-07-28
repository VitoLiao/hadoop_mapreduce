package com.mr;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;

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
import com.util.ViewlogOptionUtil;

public class ChannelLinked {

	public static void main(String[] args) throws IOException, ParseException {

		// 鑾峰緱鍚姩鍙傛暟
		ViewlogOptionUtil option = new ViewlogOptionUtil(args);

		String outputPath = "hdfs://" + option.getHdfsActiveHost() + ":" + option.getPort() + "/utsc/output/channel_link";
		
		if (HdfsOptionUtil.isPathExist(outputPath)) {
			if (HdfsOptionUtil.deletePath(outputPath)) {
				System.out.println("Delete output path success.");
			} else {
				System.out.println("Delete output path fail.");
				System.exit(1);
			}
		}
		
		ArrayList<String> filePathList = option.getViewlogPathList();
		
		// 绗竴娆apreduce
		Configuration conf01 = new Configuration();
		
		
		conf01.set("mapred.textoutputformat.separator", "|");
		//灏嗗弬鏁颁紶閫掔粰map
		conf01.set("CalcDate", option.getCalcStartDate());
		conf01.set("DateType", option.getCalcType().toString());

		Job job01 = new Job(conf01, "channel_linked_01");
		ControlledJob jobCtrl01 = new ControlledJob(conf01);
		jobCtrl01.setJob(job01);
		job01.setJarByClass(ChannelLinked.class);

		job01.setMapperClass(ChannelLinkedMapper1.class);
		job01.setReducerClass(ChannelLinkedReducer1.class);

		job01.setMapOutputKeyClass(Text.class);
		job01.setMapOutputValueClass(Text.class);
		job01.setOutputKeyClass(Text.class);
		job01.setOutputValueClass(Text.class);
		job01.setNumReduceTasks(24);
//		job01.setOutputFormatClass(GbkOutputFormat.class);

		for (int i = 0; i < filePathList.size(); i++) {
			// 鑾峰緱闇�瑕佽鍙栫殑鏂囦欢璺緞
			String filePath = filePathList.get(i);

			// 鍒ゆ柇璺緞鍦╤dfs涓婃槸鍚﹀瓨鍦�
			FileSystem fs = FileSystem.get(URI.create(filePath), conf01);
			Path path = new Path(filePath);
			if (fs.exists(path)) {
				FileInputFormat.addInputPath(job01, new Path(filePath));
			}
		}
		
//		if ((FileInputFormat.getInputPaths(job01)).length == 0) {
//			System.exit(1);
//		}
//		FileInputFormat.addInputPath(job01, new Path("hdfs://10.0.18.98:8020/utsc/input/channel_skip_count/"));
		FileOutputFormat.setOutputPath(job01, new Path(outputPath + "/channel_link_01"));

		// 绗簩娆apreduce
		Configuration conf02 = new Configuration();

		// 灏唊ey浜巚alue涔嬮棿榛樿鐨勫垎鍓茬杞寲鎴愭寚瀹氱殑绗﹀彿
		conf02.set("mapred.textoutputformat.separator", "|");

		Job job02 = new Job(conf02, "channel_linked_02");
		ControlledJob jobCtrl02 = new ControlledJob(conf02);

		jobCtrl02.setJob(job02);
		jobCtrl02.addDependingJob(jobCtrl01);

		job02.setJarByClass(ChannelLinked.class);

		job02.setMapperClass(ChannelLinkedMapper2.class);
		job02.setReducerClass(ChannelLinkedReducer2.class);

		job02.setMapOutputKeyClass(Text.class);
		job02.setMapOutputValueClass(Text.class);
		job02.setOutputKeyClass(Text.class);
		job02.setOutputValueClass(Text.class);
		job02.setNumReduceTasks(12);
//		job02.setOutputFormatClass(GbkOutputFormat.class);

		FileInputFormat.addInputPath(job02, new Path(outputPath + "/channel_link_01"));

		FileOutputFormat.setOutputPath(job02, new Path(outputPath + "/channel_link_02"));

		//绗笁娆apreduce
		Configuration conf03 = new Configuration();

		// 灏唊ey浜巚alue涔嬮棿榛樿鐨勫垎鍓茬杞寲鎴愭寚瀹氱殑绗﹀彿
		conf03.set("mapred.textoutputformat.separator", "|");

		Job job03 = new Job(conf03, "channel_linked_03");
		ControlledJob jobCtrl03 = new ControlledJob(conf03);

		jobCtrl03.setJob(job03);
		jobCtrl03.addDependingJob(jobCtrl02);

		job03.setJarByClass(ChannelLinked.class);

		job03.setMapperClass(ChannelLinkedMapper3.class);
		job03.setReducerClass(ChannelLinkedReducer3.class);

		job03.setMapOutputKeyClass(Text.class);
		job03.setMapOutputValueClass(Text.class);
		job03.setOutputKeyClass(Text.class);
		job03.setOutputValueClass(Text.class);
		job03.setNumReduceTasks(8);
//		job02.setOutputFormatClass(GbkOutputFormat.class);

		FileInputFormat.addInputPath(job03, new Path(outputPath + "/channel_link_02"));

		FileOutputFormat.setOutputPath(job03, new Path(outputPath + "/channel_link_03"));
		
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
