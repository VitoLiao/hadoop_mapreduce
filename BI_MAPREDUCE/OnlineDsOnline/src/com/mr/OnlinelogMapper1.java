package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.util.OnlinelogRowData;
import com.util.ViewlogDateUtil;

public class OnlinelogMapper1 extends Mapper<LongWritable, Text, Text, Text> {
	private Text keyText = new Text();
	private Text valueText = new Text();
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

			System.out.println("dayStartTime : " + calcStartTime + ", " + "dayEndTime: " + calcEndTime);

			// 如果登录日期和被计算日期相同
			if (ViewlogDateUtil.getDate(calcStartTime).equals(ViewlogDateUtil.getDate(onlinelogRowData.getStartTime()))) {
				isNeedCalc = true;
			}
			
//			if (calcStartTime.compareTo(onlinelogRowData.getEndTime()) < 0
//					&& calcEndTime.compareTo(onlinelogRowData.getStartTime()) > 0) {
//				isNeedCalc = true;
//			}
			
			System.out.println(calcStartTime + "," + onlinelogRowData.getEndTime() + "," + calcStartTime.compareTo(onlinelogRowData.getEndTime()));
			System.out.println(calcEndTime + "," + onlinelogRowData.getStartTime() + "," + calcEndTime.compareTo(onlinelogRowData.getStartTime()));

			if (isNeedCalc) {

				keyText.set(calcStartTime + "," + onlinelogRowData.getAreaCode() + "," + onlinelogRowData.getHdFlag());
				valueText.set(onlinelogRowData.getStartTime() + "," + onlinelogRowData.getEndTime());

				context.write(keyText, valueText);
			}
		}
	}
}
