package com.mr;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
* @author VitoLiao
* @version Apr 14, 2016 12:14:17 AM
*/
public class ChannelLinkedReducer2 extends Reducer<Text, Text, Text, Text> {
//	private Text valueInt = new Text();
	private Text keyText = new Text();
	private Text valueText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		ArrayList<String> listCode = new ArrayList<String>();
		ArrayList<String> listIntervalTime = new ArrayList<String>();
		for (Text value : values){
			String []strValue = value.toString().trim().split("\\|");
			listCode.add(strValue[0]);
			listIntervalTime.add(strValue[1]);
		}
		
		//input
		//map2锛�	KEY:	VALUE:
		//鏃ユ湡|CONTENTTYPE|TYPE|USERID	CODE|TIMEINTERVAL
		//output
		//map2: KEY:	VALUE;
		//鏃ユ湡|CONTENTTYPE|TYPE|CODE|LCODE	USERID|TIMEINTERVAL
		String []str = key.toString().trim().split("\\|");
		String strPrefix = str[0] + "|" + str[1] +"|" + str[2];
		String strUserId = str[3];
		for (int i=0;i<listCode.size();i++){
			for (int j=0;j<listCode.size();j++){
				if (i==j){}
				else{
//					System.out.println("strCode[i]= "+strCode[i]);
					keyText.set(strPrefix + "|" + listCode.get(i) + "|" + listCode.get(j));
					valueText.set(strUserId + "|" + listIntervalTime.get(j));
					context.write(keyText, valueText);
				}
			}
		}
	}
}
