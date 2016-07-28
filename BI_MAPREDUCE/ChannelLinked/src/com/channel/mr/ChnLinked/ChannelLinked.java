package com.channel.mr.ChnLinked;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.viewlog.util.HdfsOptionUtil;

public class ChannelLinked {

	public static void main(String[] args) throws IOException, ParseException {

		// ViewlogOptionUtil option = new ViewlogOptionUtil(args);

		ArrayList<String> filePathList = new ArrayList<String>();

		// local debug
		// String locationStrIn = "hdfs://10.80.248.12:8020/sampsonTest/input";
		// String locationStrOut =
		// "hdfs://10.80.248.12:8020/sampsonTest/output";
		// String locationSchedule =
		// "hdfs://10.80.248.12:8020/sampsonTest/input/schedule/";
		// cluser
		// String locationStrIn = "hdfs://10.80.248.12:8020/utsc/input_log";
		// String locationStrOut =
		// "hdfs://10.80.248.12:8020/utsc/output/ChannelLinked";
		// String locationSchedule =
		// "hdfs://10.80.248.12:8020/utsc/input_table/schedule/part-m-00000";
		// String locationOrder =
		// "hdfs://10.80.248.12:8020/utsc/input_log/Orderlog_20160522.log";

		// String DFlag = "local";
		// String DFlag = "cluster";
		String DFlag = "online";
		// String DFlag = "online1";
		String locationStrIn = "hdfs://10.80.248.12:8020/sampsonTest/input";
		String locationStrOut = "hdfs://10.80.248.12:8020/sampsonTest/output";
		String locationSchedule = "hdfs://10.80.248.12:8020/utsc/input_table/schedule/part-m-00000";

		if (DFlag.equals("local")) {
			locationStrIn = "hdfs://10.80.248.12:8020/utsc/input_log";
			locationStrOut = "hdfs://10.80.248.12:8020/utsc/output/ChannelLinked";
			locationSchedule = "hdfs://10.80.248.12:8020/utsc/input_table/schedule/part-m-00000";
		} else if (DFlag.equals("cluster")) {
			locationStrIn = "hdfs://10.80.248.12:8020/utsc/input_log";
			locationStrOut = "hdfs://10.80.248.12:8020/utsc/output/channel_link";
			locationSchedule = "hdfs://10.80.248.12:8020/utsc/input_table/schedule/part-m-00000";
		} else if (DFlag.equals("online")) {
			locationStrIn = "hdfs://10.0.18.98:8020/utsc/input_log";
			locationStrOut = "hdfs://10.0.18.98:8020/utsc/output/channel_link";
			locationSchedule = "hdfs://10.0.18.98:8020/utsc/input_table/schedule/part-m-00000";
		}

		String calType = "0";
		String calNum = "0";
		String IPPort = "0";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date now = new Date();
		String startDate = sdf.format(now);
		if (args.length % 2 == 0) {
			for (int i = 0; i < args.length; i++) {
				if (args[i].trim().equals("-s")) {
					startDate = args[i + 1];
				} else if (args[i].trim().equals("-t")) {
					calType = args[i + 1];
				} else if (args[i].trim().equals("-n")) {
					calNum = args[i + 1];
				} else if (args[i].trim().equals("-host")) {
					IPPort = args[i + 1];
				}
				i += 1;
			}
		}

		if (calType.equals("0")) {
			calType = "d";
		}
		if (calNum.equals("0")) {
			calNum = "1";
		}

		if (IPPort.equals("0")) {

		} else {
			locationStrIn = "hdfs://" + IPPort + "/utsc/input_log";
			locationStrOut = "hdfs://" + IPPort + "/utsc/output/ChannelLinked";
			locationSchedule = "hdfs://" + IPPort + "/utsc/input_table/schedule/part-m-00000";
		}

		ArrayList<String> neededDate = new ArrayList<String>();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(sdf.parse(startDate));
		StringBuffer calDate = new StringBuffer();
		for (int t = 0; t < Integer.valueOf(calNum); t++) {
			neededDate.add(sdf.format(calendar.getTime()));
			if (calDate.length() == 0) {
				calDate.append(sdf.format(calendar.getTime()));
			} else {
				calDate.append("##" + sdf.format(calendar.getTime()));
			}
			calendar.add(Calendar.DATE, -1);
		}

		ArrayList<String> orderList = new ArrayList<>();

		for (int i = 0; i < neededDate.size(); i++) {
			String tmpDate = neededDate.get(i).substring(0, 10).replace("-", "");
			String filePath = locationStrIn + "/Contentviewlog_" + tmpDate + ".log";
			System.out.println("filePath : " + filePath);
			filePathList.add(filePath);

			String oString = locationStrIn + "/Orderlog_" + tmpDate + ".log";
			System.out.println("Orderlog filePath : " + oString);
			orderList.add(oString);
		}

		if (HdfsOptionUtil.isPathExist(new String(locationStrOut))) {
			if (HdfsOptionUtil.deletePath(new String(locationStrOut))) {
				System.out.println("Delete output path success.");
			} else {
				System.out.println("Delete output path fail.");
				System.exit(1);
			}
		}

		Configuration conf01 = new Configuration();
		Configuration conf02 = new Configuration();
		Configuration conf03 = new Configuration();
		Configuration conf04 = new Configuration();
		Configuration conf05 = new Configuration();
		Configuration conf06 = new Configuration();
		Configuration conf07 = new Configuration();
		Configuration conf08 = new Configuration();
		Configuration conf09 = new Configuration();
		Configuration conf10 = new Configuration();
		JobControl jobControl = new JobControl("ctr");

		conf01.set("mapred.textoutputformat.separator", "|");
		conf01.set("calType", calType);
		conf01.set("calDate", calDate.toString());
		Job job01 = Job.getInstance(conf01, "channel_linked_01");
		ControlledJob jobCtrl01 = new ControlledJob(conf01);
		jobCtrl01.setJob(job01);
		job01.setJarByClass(ChannelLinked.class);
		job01.setMapperClass(ChannelLinkedMapper1.class);
		job01.setReducerClass(ChannelLinkedReducer1.class);
		job01.setMapOutputKeyClass(Text.class);
		job01.setMapOutputValueClass(Text.class);
		job01.setOutputKeyClass(Text.class);
		job01.setOutputValueClass(Text.class);
		job01.setNumReduceTasks(36);

		for (int i = 0; i < filePathList.size(); i++) {
			String filePath = filePathList.get(i);

			FileSystem fs = FileSystem.get(URI.create(filePath), conf01);
			Path path = new Path(filePath);
			if (fs.exists(path)) {
				FileInputFormat.addInputPaths(job01, filePath);
			}
		}
		FileOutputFormat.setOutputPath(job01, new Path(locationStrOut + "/channel_linked_01"));

		MultipleOutputs.addNamedOutput(job01, "c", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job01, "t", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job01, "ot", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job01, "ct", TextOutputFormat.class, Text.class, Text.class);

		// Configuration conf02 = new Configuration();
		conf02.set("mapred.textoutputformat.separator", "|");
		conf02.set("calDate", calDate.toString());
		Job job02 = Job.getInstance(conf02, "channel_linked_02");
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
		// job02.setNumReduceTasks(36);

		FileInputFormat.addInputPath(job02, new Path(locationSchedule));
		FileOutputFormat.setOutputPath(job02, new Path(locationStrOut + "/channel_linked_02"));

		// Configuration conf03 = new Configuration();

		conf03.set("mapred.textoutputformat.separator", "###");
		Job job03 = Job.getInstance(conf03, "channel_linked_03");
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
		job03.setNumReduceTasks(18);

		FileInputFormat.addInputPath(job03, new Path(locationStrOut + "/channel_linked_02"));
		FileInputFormat.addInputPath(job03, new Path(locationStrOut + "/channel_linked_01/ot"));
		FileOutputFormat.setOutputPath(job03, new Path(locationStrOut + "/channel_linked_03"));

		// Configuration conf04 = new Configuration();

		conf04.set("mapred.textoutputformat.separator", "|");
		Job job04 = Job.getInstance(conf04, "channel_linked_04");
		ControlledJob jobCtrl04 = new ControlledJob(conf04);
		jobCtrl04.setJob(job04);
		jobCtrl04.addDependingJob(jobCtrl03);
		job04.setJarByClass(ChannelLinked.class);
		job04.setMapperClass(ChannelLinkedMapper4.class);
		job04.setReducerClass(ChannelLinkedReducer4.class);
		job04.setMapOutputKeyClass(Text.class);
		job04.setMapOutputValueClass(Text.class);
		job04.setOutputKeyClass(Text.class);
		job04.setOutputValueClass(Text.class);
		job04.setNumReduceTasks(36);

		FileInputFormat.addInputPath(job04, new Path(locationStrOut + "/channel_linked_03"));
		FileOutputFormat.setOutputPath(job04, new Path(locationStrOut + "/channel_linked_04"));

		//
		if (HdfsOptionUtil.isPathExist(new String(orderList.get(0)))) {

			conf05.set("mapred.textoutputformat.separator", "|");
			Job job05 = Job.getInstance(conf05, "channel_linked_05");
			ControlledJob jobCtrl05 = new ControlledJob(conf05);
			jobCtrl05.setJob(job05);
			jobCtrl05.addDependingJob(jobCtrl04);
			job05.setJarByClass(ChannelLinked.class);
			job05.setMapperClass(ChannelLinkedMapper5.class);
			job05.setReducerClass(ChannelLinkedReducer5.class);
			job05.setMapOutputKeyClass(Text.class);
			job05.setMapOutputValueClass(Text.class);
			job05.setOutputKeyClass(Text.class);
			job05.setOutputValueClass(Text.class);
			job05.setNumReduceTasks(36);

			for (int i = 0; i < orderList.size(); i++) {
				String filePath = orderList.get(i);

				FileSystem fs = FileSystem.get(URI.create(filePath), conf05);
				Path path = new Path(filePath);
				if (fs.exists(path)) {
					FileInputFormat.addInputPaths(job05, filePath);
				}
			}

			FileOutputFormat.setOutputPath(job05, new Path(locationStrOut + "/channel_linked_05"));

			conf06.set("mapred.textoutputformat.separator", "###");
			Job job06 = Job.getInstance(conf06, "channel_linked_06");
			ControlledJob jobCtrl06 = new ControlledJob(conf06);
			jobCtrl06.setJob(job06);
			jobCtrl06.addDependingJob(jobCtrl05);
			job06.setJarByClass(ChannelLinked.class);
			job06.setMapperClass(ChannelLinkedMapper6.class);
			job06.setReducerClass(ChannelLinkedReducer6.class);
			job06.setMapOutputKeyClass(Text.class);
			job06.setMapOutputValueClass(Text.class);
			job06.setOutputKeyClass(Text.class);
			job06.setOutputValueClass(Text.class);
			job06.setNumReduceTasks(36);

			FileInputFormat.addInputPath(job06, new Path(locationStrOut + "/channel_linked_05"));
			FileInputFormat.addInputPath(job06, new Path(locationStrOut + "/channel_linked_01/c"));
			FileInputFormat.addInputPath(job06, new Path(locationStrOut + "/channel_linked_01/t"));
			FileInputFormat.addInputPath(job06, new Path(locationStrOut + "/channel_linked_01/ct"));
			FileInputFormat.addInputPath(job06, new Path(locationStrOut + "/channel_linked_04"));
			FileOutputFormat.setOutputPath(job06, new Path(locationStrOut + "/channel_linked_06"));

			conf07.set("mapred.textoutputformat.separator", "|");
			Job job07 = Job.getInstance(conf07, "channel_linked_07");
			ControlledJob jobCtrl07 = new ControlledJob(conf07);
			jobCtrl07.setJob(job07);
			jobCtrl07.addDependingJob(jobCtrl06);
			job07.setJarByClass(ChannelLinked.class);
			job07.setMapperClass(ChannelLinkedMapper7.class);
			job07.setReducerClass(ChannelLinkedReducer7.class);
			job07.setMapOutputKeyClass(Text.class);
			job07.setMapOutputValueClass(Text.class);
			job07.setOutputKeyClass(Text.class);
			job07.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job07, new Path(locationStrOut + "/channel_linked_06"));
			FileOutputFormat.setOutputPath(job07, new Path(locationStrOut + "/channel_linked_07"));

			conf08.set("mapred.textoutputformat.separator", "###");
			Job job08 = Job.getInstance(conf08, "channel_linked_08");
			ControlledJob jobCtrl08 = new ControlledJob(conf08);
			jobCtrl08.setJob(job08);
			jobCtrl08.addDependingJob(jobCtrl07);
			job08.setJarByClass(ChannelLinked.class);
			job08.setMapperClass(ChannelLinkedMapper8.class);
			job08.setReducerClass(ChannelLinkedReducer8.class);
			job08.setMapOutputKeyClass(Text.class);
			job08.setMapOutputValueClass(Text.class);
			job08.setOutputKeyClass(Text.class);
			job08.setOutputValueClass(Text.class);
			job08.setNumReduceTasks(108);

			FileInputFormat.addInputPath(job08, new Path(locationStrOut + "/channel_linked_01/c"));
			FileInputFormat.addInputPath(job08, new Path(locationStrOut + "/channel_linked_01/t"));
			FileInputFormat.addInputPath(job08, new Path(locationStrOut + "/channel_linked_01/ct"));
			FileInputFormat.addInputPath(job08, new Path(locationStrOut + "/channel_linked_04"));
			FileOutputFormat.setOutputPath(job08, new Path(locationStrOut + "/channel_linked_08"));

			conf09.set("mapred.textoutputformat.separator", "|");
			Job job09 = Job.getInstance(conf09, "channel_linked_09");
			ControlledJob jobCtrl09 = new ControlledJob(conf09);
			jobCtrl09.setJob(job09);
			jobCtrl09.addDependingJob(jobCtrl08);
			job09.setJarByClass(ChannelLinked.class);
			job09.setMapperClass(ChannelLinkedMapper9.class);
			job09.setReducerClass(ChannelLinkedReducer9.class);
			job09.setMapOutputKeyClass(Text.class);
			job09.setMapOutputValueClass(Text.class);
			job09.setOutputKeyClass(Text.class);
			job09.setOutputValueClass(Text.class);
			job09.setNumReduceTasks(108);

			FileInputFormat.addInputPath(job09, new Path(locationStrOut + "/channel_linked_08"));
			FileOutputFormat.setOutputPath(job09, new Path(locationStrOut + "/channel_linked_09"));

			conf10.set("mapred.textoutputformat.separator", "|");
			Job job10 = Job.getInstance(conf10, "channel_linked_10");
			ControlledJob jobCtrl10 = new ControlledJob(conf10);
			jobCtrl10.setJob(job10);
			jobCtrl10.addDependingJob(jobCtrl09);
			job10.setJarByClass(ChannelLinked.class);
			job10.setMapperClass(ChannelLinkedMapper91.class);
			job10.setReducerClass(ChannelLinkedReducer91.class);
			job10.setMapOutputKeyClass(Text.class);
			job10.setMapOutputValueClass(Text.class);
			job10.setOutputKeyClass(Text.class);
			job10.setOutputValueClass(NullWritable.class);

			FileInputFormat.addInputPath(job10, new Path(locationStrOut + "/channel_linked_09"));
			FileInputFormat.addInputPath(job10, new Path(locationStrOut + "/channel_linked_07"));
			FileOutputFormat.setOutputPath(job10, new Path(locationStrOut + "/channel_linked_10"));

			// JobControl jobControl = new JobControl("ctr");
			jobControl.addJob(jobCtrl01);
			jobControl.addJob(jobCtrl02);
			jobControl.addJob(jobCtrl03);
			jobControl.addJob(jobCtrl04);
			jobControl.addJob(jobCtrl05);
			jobControl.addJob(jobCtrl06);
			jobControl.addJob(jobCtrl07);
			jobControl.addJob(jobCtrl08);
			jobControl.addJob(jobCtrl09);
			jobControl.addJob(jobCtrl10);
		} else {

			System.out.println("Warming : There is no OrderLog in path.");
			System.out.println("Job channel_linked_05,channel_linked_06,channel_linked_07 is skiped.");
			conf08.set("mapred.textoutputformat.separator", "###");
			Job job08 = Job.getInstance(conf08, "channel_linked_05~08");
			ControlledJob jobCtrl08 = new ControlledJob(conf08);
			jobCtrl08.setJob(job08);
			jobCtrl08.addDependingJob(jobCtrl04);
			job08.setJarByClass(ChannelLinked.class);
			job08.setMapperClass(ChannelLinkedMapper8.class);
			job08.setReducerClass(ChannelLinkedReducer8.class);
			job08.setMapOutputKeyClass(Text.class);
			job08.setMapOutputValueClass(Text.class);
			job08.setOutputKeyClass(Text.class);
			job08.setOutputValueClass(Text.class);
			job08.setNumReduceTasks(216);

			FileInputFormat.addInputPath(job08, new Path(locationStrOut + "/channel_linked_01/c"));
			FileInputFormat.addInputPath(job08, new Path(locationStrOut + "/channel_linked_01/t"));
			FileInputFormat.addInputPath(job08, new Path(locationStrOut + "/channel_linked_01/ct"));
			FileInputFormat.addInputPath(job08, new Path(locationStrOut + "/channel_linked_04"));
			FileOutputFormat.setOutputPath(job08, new Path(locationStrOut + "/channel_linked_08"));

			conf09.set("mapred.textoutputformat.separator", "|");
			Job job09 = Job.getInstance(conf09, "channel_linked_09");
			ControlledJob jobCtrl09 = new ControlledJob(conf09);
			jobCtrl09.setJob(job09);
			jobCtrl09.addDependingJob(jobCtrl08);
			job09.setJarByClass(ChannelLinked.class);
			job09.setMapperClass(ChannelLinkedMapper9.class);
			job09.setReducerClass(ChannelLinkedReducer9.class);
			job09.setMapOutputKeyClass(Text.class);
			job09.setMapOutputValueClass(Text.class);
			job09.setOutputKeyClass(Text.class);
			job09.setOutputValueClass(Text.class);
			job09.setNumReduceTasks(216);

			FileInputFormat.addInputPath(job09, new Path(locationStrOut + "/channel_linked_08"));
			FileOutputFormat.setOutputPath(job09, new Path(locationStrOut + "/channel_linked_09"));

			conf10.set("mapred.textoutputformat.separator", "|");
			Job job10 = Job.getInstance(conf10, "channel_linked_10");
			ControlledJob jobCtrl10 = new ControlledJob(conf10);
			jobCtrl10.setJob(job10);
			jobCtrl10.addDependingJob(jobCtrl09);
			job10.setJarByClass(ChannelLinked.class);
			job10.setMapperClass(ChannelLinkedMapper91.class);
			job10.setReducerClass(ChannelLinkedReducer91.class);
			job10.setMapOutputKeyClass(Text.class);
			job10.setMapOutputValueClass(Text.class);
			job10.setOutputKeyClass(Text.class);
			job10.setOutputValueClass(NullWritable.class);

			FileInputFormat.addInputPath(job10, new Path(locationStrOut + "/channel_linked_09"));
//			FileInputFormat.addInputPath(job10, new Path(locationStrOut + "/channel_linked_07"));
			FileOutputFormat.setOutputPath(job10, new Path(locationStrOut + "/channel_linked_10"));

			// JobControl jobControl = new JobControl("ctr");
			jobControl.addJob(jobCtrl01);
			jobControl.addJob(jobCtrl02);
			jobControl.addJob(jobCtrl03);
			jobControl.addJob(jobCtrl04);
			jobControl.addJob(jobCtrl08);
			jobControl.addJob(jobCtrl09);
			jobControl.addJob(jobCtrl10);
		}

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
