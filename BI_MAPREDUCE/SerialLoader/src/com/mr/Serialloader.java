package com.mr;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.util.HdfsOptionUtil;
import com.util.ViewlogOptionUtil;

public class Serialloader {

	public static void main(String[] args)
			throws IOException, InterruptedException, ClassNotFoundException, ParseException {

		// �����������
		ViewlogOptionUtil option = new ViewlogOptionUtil(args);

		ArrayList<String> filePathList = new ArrayList<String>();

		filePathList = option.getViewlogPathList();

		String outputPath = "hdfs://" + option.getHdfsActiveHost() + ":" + option.getPort() + "/utsc/output/serial_loader";
		
		// ������Ŀ¼���ڣ���ɾ�����Ŀ¼
		if (HdfsOptionUtil.isPathExist(outputPath)) {
			if (HdfsOptionUtil.deletePath(outputPath)) {
				System.out.println("Delete output path success.");
			} else {
				System.out.println("Delete output path fail.");
				System.exit(1);
			}
		}

		Configuration conf = new Configuration();

		// ��key��value֮��Ĭ�ϵķָ��ת����ָ���ķ���
		conf.set("mapred.textoutputformat.separator", "|");
		conf.set("CalcDate", option.getCalcStartDate());
		conf.set("DateType", option.getCalcType().toString());
		
		Job job = new Job(conf, "serialloader");

		job.setJarByClass(Serialloader.class);

		job.setMapperClass(SerialMapper.class);
		job.setReducerClass(SerialReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(24);

		for (int i = 0; i < filePathList.size(); i++) {
			// �����Ҫ��ȡ���ļ�·��a
			String filePath = filePathList.get(i);

			// �ж�·����hdfs���Ƿ����
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
	}
}
