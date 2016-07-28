package com.mr;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.util.ReduceSort;
import com.util.ScheduleReduceData;
import com.util.ViewlogDateUtil;
import com.util.ViewlogReduceData;

/**
 * @author VitoLiao
 * @version 2016��2��29�� ����10:02:59
 */

public class ScheduleVitalityReducer1 extends Reducer<Text, Text, Text, IntWritable> {
	private Text keyText = new Text();
	private IntWritable valueText = new IntWritable();
	private final static String scheduleFileFlag = "s";
	private final static String viewlogFileFlag = "v";
	private final static int one = 1;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		ArrayList<ScheduleReduceData> scheduleList = new ArrayList<ScheduleReduceData>();
		ArrayList<ViewlogReduceData> viewlogList = new ArrayList<ViewlogReduceData>();
		
		for (Text value : values) {
			String valueStr = value.toString();
			String[] str = value.toString().split("\\|");

			String fileFlag = str[0];

			if (fileFlag.equals(scheduleFileFlag)) {
				scheduleList.add(new ScheduleReduceData(valueStr.substring(str[0].length() + 1, valueStr.length())));
			} else if (fileFlag.equals(viewlogFileFlag)) {
				viewlogList.add(new ViewlogReduceData(valueStr.substring(str[0].length() + 1, valueStr.length())));
			}

		}

		// ��schedule���տ�ʼʱ�������������
		Collections.sort(scheduleList, new ReduceSort());
		
//		for (ScheduleReduceData a : scheduleList) {
//			System.out.println(a.getScheduleCode() + ", " + a.getStartTime() + ", " + a.getEndTime());
//		}

		for (int j = 0; j < viewlogList.size(); j++) {
			ViewlogReduceData viewlogData = viewlogList.get(j);
			for (int i = 0; i < scheduleList.size(); i++) {
				ScheduleReduceData scheduleData = scheduleList.get(i);
				long viewInterval = 0;

				if (viewlogData.getStartTime().compareTo(scheduleData.getEndTime()) > 0) {
					continue;
				}
				
				if (viewlogData.getEndTime().compareTo(scheduleData.getStartTime()) < 0) {
					break;
				}
				
				try {
					// �����û��տ���Ŀʱ��
					viewInterval = ViewlogDateUtil.intersectionInterval(
							ViewlogDateUtil.str2date(scheduleData.getStartTime()),
							ViewlogDateUtil.str2date(scheduleData.getEndTime()),
							ViewlogDateUtil.str2date(viewlogData.getStartTime()),
							ViewlogDateUtil.str2date(viewlogData.getEndTime()));
					
					if (viewInterval > 0) {
						keyText.set(viewlogData.getStartDateStr() + "|" + viewlogData.getUserId() + "|" + key.toString()
								+ "|" + scheduleData.getScheduleCode() + "|" + viewlogData.getAreaCode() + "|"
								+ viewlogData.getHdflag());
	
						valueText.set(one);
	
						context.write(keyText, valueText);
					}

				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
