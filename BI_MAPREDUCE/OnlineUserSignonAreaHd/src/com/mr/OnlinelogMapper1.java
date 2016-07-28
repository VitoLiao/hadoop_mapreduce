package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.util.OnlinelogRowData;

public class OnlinelogMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text keyText = new Text();
	private IntWritable valueInt = new IntWritable(1);
	private String calcStartTime = null;
	private String calcEndTime = null;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (value.getLength() > 0) {
			OnlinelogRowData onlinelogRowData = null;

			try {
				onlinelogRowData = new OnlinelogRowData(value.toString(), ",");
			} catch (Exception e) {
				System.out.println(e.getMessage());
				return;
			}

			boolean isNeedCalc = false;

			calcStartTime = context.getConfiguration().get("calcStartTime");
			calcEndTime = context.getConfiguration().get("calcEndTime");

			System.out.println("dayStartTime : " + calcStartTime + ", " + "dayEndTime" + calcEndTime);

			// 是否和被运算区间存在交集
			if (calcStartTime.compareTo(onlinelogRowData.getEndTime()) < 0
					&& calcEndTime.compareTo(onlinelogRowData.getStartTime()) > 0) {
				isNeedCalc = true;
			}
			
			System.out.println(calcStartTime + "," + onlinelogRowData.getEndTime() + "," + calcStartTime.compareTo(onlinelogRowData.getEndTime()));
			System.out.println(calcEndTime + "," + onlinelogRowData.getStartTime() + "," + calcEndTime.compareTo(onlinelogRowData.getStartTime()));

			if (isNeedCalc) {

				keyText.set(calcStartTime + "|" + onlinelogRowData.getUserId() + "|" + onlinelogRowData.getAreaCode() + "|" + onlinelogRowData.getHdFlag());

				context.write(keyText, valueInt);
			}
		}
	}
}
