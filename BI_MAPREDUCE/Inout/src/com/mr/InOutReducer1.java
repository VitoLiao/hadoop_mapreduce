package com.mr;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.util.RemoveDuplicateData;
import com.util.ViewlogDateUtil;

/**
 * @author VitoLiao
 * @version 2016��4��13�� ����2:05:20
 */
public class InOutReducer1 extends Reducer<Text, Text, Text, Text> {
	// ��¼��key��ÿСʱ�����û��б���¼���û�ID
	private int hourCount = 24;
	private Text valueText = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		ArrayList<ArrayList<String>> viewInHourUsersList = new ArrayList<ArrayList<String>>();
		ArrayList<ArrayList<String>> viewOutHourUsersList = new ArrayList<ArrayList<String>>();
		int[] inUserCountArray = new int[hourCount];
		int[] outUserCountArray = new int[hourCount];
		
		for (int i = 0; i < hourCount; i++) {
			ArrayList<String> tmpList = new ArrayList<String>();
			viewInHourUsersList.add(tmpList);
		}
		
		for (int i = 0; i < hourCount; i++) {
			ArrayList<String> tmpList = new ArrayList<String>();
			viewOutHourUsersList.add(tmpList);
		}

		for (Text value : values) {
			String[] str = value.toString().split("\\|");

			String userId = str[0];
			String startTime = str[1];
			String endTime = str[2];

			int inHourFlag = 0;
			int outHourFlag = 0;
			
			try {
				inHourFlag = ViewlogDateUtil.getHourOfDay(ViewlogDateUtil.str2date(startTime));
				outHourFlag = ViewlogDateUtil.getHourOfDay(ViewlogDateUtil.str2date(endTime));
			} catch (ParseException e1) {
				e1.printStackTrace();
			}
			
			(viewInHourUsersList.get(inHourFlag)).add(userId);
			(viewOutHourUsersList.get(outHourFlag)).add(userId);

		}
		
		for (int i = 0; i < hourCount; i++) {
			inUserCountArray[i] = RemoveDuplicateData.getUniqueSize(viewInHourUsersList.get(i));
			outUserCountArray[i] = RemoveDuplicateData.getUniqueSize(viewOutHourUsersList.get(i));
		}

		// VIN, VOUT
		String tmpStr = "";
		
		for (int i = 0; i < hourCount; i++) {
			tmpStr += String.valueOf(inUserCountArray[i]);
			tmpStr += "|";
		}
		
		for (int i = 0; i < hourCount; i++) {
			tmpStr += String.valueOf(outUserCountArray[i]);
			if (i != hourCount - 1) {
				tmpStr += "|";
			}
		}		
		
		valueText.set(tmpStr);
		
		context.write(key, valueText);
	}
}
