package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
* @author VitoLiao
* @version 2016��3��1�� ����2:07:47
*/
public class ScheduleTimeMapper3 extends Mapper<LongWritable, Text, Text, IntWritable>{
	private Text keyText = new Text();
	private static final IntWritable one = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String []str = value.toString().trim().split(",");
		
		String []keyStr = str[0].split("\\|");
		
		String tmpKey = "";
		
		//ȥ���û�ID
		for(int i = 0; i < keyStr.length; i++) {
			if (i != 1) {
				tmpKey += keyStr[i];
				if (i != keyStr.length - 1) {
					tmpKey += "|";
				}
			}
		}
		
		//keyΪmonthday + channelcode + areacode + hdflag + datetype + clicktimes
		keyText.set(tmpKey + "|" + str[1]);
		
		context.write(keyText, one);		
	}
}
