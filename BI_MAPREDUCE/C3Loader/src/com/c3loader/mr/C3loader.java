package com.c3loader.mr;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.util.HdfsOptionUtil;
import com.util.ViewlogOptionUtil;

import org.apache.hadoop.mapreduce.Job;

public class C3loader {

	public static void main(String[] args)
			throws IOException, InterruptedException, ClassNotFoundException, ParseException {

		// 获得配置和启动参数
		ViewlogOptionUtil option = new ViewlogOptionUtil(args);

		// 获得viewlog文件列表
		ArrayList<String> filePathList = option.getViewlogPathList();

		// HDFS输出路径
		String outputPath = "hdfs://" + option.getHdfsActiveHost() + ":" + option.getPort() + "/utsc/output/c3_loader";

		// 如果输出路径存在，则先删除此路径，否则无法输出
		if (HdfsOptionUtil.isPathExist(outputPath)) {
			if (HdfsOptionUtil.deletePath(outputPath)) {
				System.out.println("Delete output path success.");
			} else {
				System.out.println("Delete output path fail.");
				System.exit(1);
			}
		}

		Configuration conf = new Configuration();

		conf.set("mapred.textoutputformat.separator", "|");
		conf.set("CalcDate", option.getCalcStartDate());
		conf.set("DateType", option.getCalcType().toString());

		Job job = new Job(conf, "c3loader");

		job.setJarByClass(C3loader.class);

		job.setMapperClass(C3loaderMapper.class);
		job.setReducerClass(C3loaderReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(24);

		for (int i = 0; i < filePathList.size(); i++) {
			String filePath = filePathList.get(i);

			if (HdfsOptionUtil.isPathExist(filePath)) {
				FileInputFormat.addInputPath(job, new Path(filePath));
			} else {
				System.out.println("File not exist, skip it : " + filePath);
				continue;
			}
		}

		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.waitForCompletion(true);
		System.out.println("Finished");
		System.exit(0);
	}
}
