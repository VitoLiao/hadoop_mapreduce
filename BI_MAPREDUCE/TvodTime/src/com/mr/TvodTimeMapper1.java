package com.mr;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.util.ViewlogDateUtil;
import com.util.ViewlogRowData;
import com.util.ViewlogRowDataSplit;
import com.util.ViewlogStringUtil;

/**
* @author VitoLiao
* @version 2016��3��3�� ����3:18:21
*/
public class TvodTimeMapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{
	private Text keyText = new Text();
	private IntWritable valueInt = new IntWritable();	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (value.getLength() > 0) {
			
			Text line = ViewlogStringUtil.transform(value, "GBK");
			
			ViewlogRowData viewlogRowData = null;

			try {
				viewlogRowData = new ViewlogRowData(line.toString(), ",");
			} catch (Exception e) {
				System.out.println(e.getMessage());
				return;
			}
			
			ArrayList<ViewlogRowData> dataArray = null;
			
			try {
				dataArray = ViewlogRowDataSplit.split(viewlogRowData);
			} catch (ParseException e) {
				e.printStackTrace();
				System.out.println("Parse error, line : " + line.toString());
				return;				
			}
			
			if (viewlogRowData.getLogType().equals("t")) {
				for (int i = 0; i < dataArray.size(); i++) {
					ViewlogRowData tmpData = dataArray.get(i);
					
					boolean isNeedCalc = false;
					String startDate = null;

					startDate = context.getConfiguration().get("CalcDate");
					if (startDate.equals(ViewlogDateUtil.getDate(tmpData.getStartTime()))) {
						isNeedCalc = true;
					}
					
					if (null == tmpData.getParentObjectCode()
							|| 0 == tmpData.getParentObjectCode().length()
							|| false == ViewlogStringUtil.isNumeric(tmpData.getParentObjectCode())) {
						isNeedCalc = false;
					}
					
					if (isNeedCalc) {		
						startDate += " 00:00:00.0";
						
						keyText.set(startDate + "|" + tmpData.getUserId() + "|" + tmpData.getParentObjectCode().trim() + "|"+ tmpData.getMediaCode() + "|" + tmpData.getAreaCode() + "|" +  tmpData.getHdFlag());
						valueInt.set(tmpData.getTimeInterval());
						
						context.write(keyText, valueInt);
					}
				}			
			}
		}
	}
}
