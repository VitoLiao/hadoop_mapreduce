package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
* @author VitoLiao
* @version Apr 14, 2016 12:13:44 AM
*/
public class ChannelLinkedMapper2 extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text keyText = new Text();
	private Text valueText = new Text();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		//intput
		//map2锛�	KEY:	VALUE:
		//鏃ユ湡|CONTENTTYPE|TYPE|CODE|USERID|TIMEINTERVAL
		//output
		//map2锛�	KEY:	VALUE:
		//鏃ユ湡|CONTENTTYPE|TYPE|USERID	CODE|TIMEINTERVAL
		
		//鍙鐞嗗唴瀹逛笉涓虹┖鐨勬暟鎹�
		if (value.getLength() > 0) {
			String []str = value.toString().trim().split("\\|");
			keyText.set(str[0] + "|"
					+ str[1] + "|"
					+ str[2] + "|"
					+ str[4] + "|");
			valueText.set(str[3] + "|"+ str[5]);
			context.write(keyText, valueText);
//			System.out.println("keytext= " +keyText);
//			System.out.println("value= " +valueText);
		}
	}

}
