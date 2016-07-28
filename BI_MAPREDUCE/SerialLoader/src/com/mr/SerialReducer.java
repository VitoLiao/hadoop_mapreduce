package com.mr;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.util.RemoveDuplicateData;

public class SerialReducer extends Reducer<Text, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();
	private DecimalFormat df = new DecimalFormat("#.###");
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		//收视超过5分钟的时长
		int viewDurationGt5MinTimes = 0;

		//收视超过5分钟的人次
		int viewDurationGt5Min = 0;

		//收视超过5分钟的人数
		int viewDurationGt5MinUser = 0;
		
		//收视超过5分钟的人数列表
		ArrayList<String> viewDurationGt5MinUserList = new ArrayList<String>();
		
		//收视总时长
		int viewTotalDuration = 0;

		//收视总人次
		int viewTotalTimes = 0;

		//收视总人数
		int viewTotalUsers = 0;

		//收视总人数列表
		ArrayList<String> viewUsersList = new ArrayList<String>();

		//每小时收视总人数列表
		ArrayList<ArrayList<String>> viewHourUsersList = new ArrayList<ArrayList<String>>();

		for (int i = 0; i < DayInfomation.HOUR_COUNT; i++) {
			ArrayList<String> tmpList = new ArrayList<String>();
			viewHourUsersList.add(tmpList);
		}

		//每小时收视时长
		int[] viewDurationSum = new int[DayInfomation.HOUR_COUNT];

		//每小时收视人数
		int[] viewDurationUsersSum = new int[DayInfomation.HOUR_COUNT];

		//每小时收视人次
		int[] viewDurationTimesSum = new int[DayInfomation.HOUR_COUNT];

		for (Text value : values) {
			String[] valueArray = value.toString().trim().split("\\|");

			String userId = valueArray[0];

			viewUsersList.add(userId);

			//计算每个用户每小时的收视时长
			int[] viewDuration = new int[DayInfomation.HOUR_COUNT];
			for (int i = 0; i < DayInfomation.HOUR_COUNT; i++) {
				viewDuration[i] = Integer.parseInt(valueArray[i + 1]);
			}

			//计算每个用户收视超过5分钟的时长
			viewDurationGt5Min += Integer.parseInt(valueArray[valueArray.length - 1]);

			//计算收视超过5分钟的人次
			if (viewDurationGt5Min > 0) {
				viewDurationGt5MinUserList.add(userId);
				viewDurationGt5MinTimes++;
			}

			//汇总该用户的收视总时长和24小时收视时长
			for (int i = 0; i < DayInfomation.HOUR_COUNT; i++) {
				if (viewDuration[i] > 0) {
					(viewHourUsersList.get(i)).add(userId);
					viewDurationSum[i] += viewDuration[i];
					viewTotalDuration += viewDuration[i];
				}
			}
		}

		//计算每小时收视人次和人数
		for (int i = 0; i < DayInfomation.HOUR_COUNT; i++) {
			viewDurationTimesSum[i] = (viewHourUsersList.get(i)).size();
			viewDurationUsersSum[i] = RemoveDuplicateData.getUniqueSize(viewHourUsersList.get(i));
		}

		//计算收视总人次
		viewTotalTimes = viewUsersList.size();

		//计算收视总人数
		viewTotalUsers = RemoveDuplicateData.getUniqueSize(viewUsersList);
		
		//计算收视超过5分钟的人数
		viewDurationGt5MinUser = RemoveDuplicateData.getUniqueSize(viewDurationGt5MinUserList);

		String viewDurationStr = "";
		String viewDurationTimesStr = "";
		String viewDurationUsersStr = "";
		for (int i = 0; i < DayInfomation.HOUR_COUNT; i++) {
			viewDurationStr += viewDurationSum[i];
			viewDurationTimesStr += viewDurationTimesSum[i];
			viewDurationUsersStr += viewDurationUsersSum[i];

			if (i != DayInfomation.HOUR_COUNT - 1) {
				viewDurationStr += "|";
				viewDurationTimesStr += "|";
				viewDurationUsersStr += "|";
			}
		}

		String[] str = key.toString().split("\\|");

		valueText.set(viewTotalTimes + "|" + viewTotalDuration + "|" + viewDurationGt5Min + "|"
				+ viewDurationGt5MinTimes + "|" + viewDurationGt5MinUser + "|" + viewTotalUsers + "|" + viewDurationStr + "|" + "0" + "|"
				+ df.format(((float) viewTotalTimes / (float) viewTotalUsers)) + "|"
				+ df.format(((float) viewTotalDuration / (float) viewTotalUsers)) + "|"
				+ viewDurationUsersStr + "|" + viewDurationTimesStr + "|" + str[5] + "|" + str[6] + "|" + str[7]);

		String flag = null;
		
		if (str[3].equalsIgnoreCase("v")) {
			flag = "1";
		} else if (str[3].equalsIgnoreCase("c")) {
			flag = "2";
		} else if (str[3].equalsIgnoreCase("t")) {
			flag = "3";
		} else if (str[3].equalsIgnoreCase("p")) {
			flag = "8";
		} else if (str[3].equalsIgnoreCase("s")) {
			flag = "9";
		}
		keyText.set(str[0] + "|" + str[1] + "|" + str[2] + "|" + flag + "|" + str[4]);

		context.write(keyText, valueText);
	}
}
