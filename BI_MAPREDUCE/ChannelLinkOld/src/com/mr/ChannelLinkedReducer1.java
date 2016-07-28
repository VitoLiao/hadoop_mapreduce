package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
* @author VitoLiao
* @version Apr 14, 2016 12:14:03 AM
*/
public class ChannelLinkedReducer1 extends Reducer<Text, Text, Text, Text>{
	
	Text valueInt = new Text();
	private Text keyText = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int timesum = 0;
		for (Text value : values){
			timesum += Integer.valueOf(value.toString());
		}
		keyText.set(key);
		valueInt.set(String.valueOf(timesum));
		context.write(keyText, valueInt);
//		System.out.println("keytext= " +keyText);
//		System.out.println("value= " +valueInt);
		
		//value的格式：|BAll|B1|B2|B3|B4|B5|B6|B7|B8|B9|B10|B11|B12|B13|B14|B15|B16|B17|B18|B19|B20|B21|B22|B23|B24
//		int[] timePlay = {0, 0,0,0,0,0,0, 0,0,0,0,0,0, 0,0,0,0,0,0, 0,0,0,0,0,0};
//		int[] timeBreak = {0, 0,0,0,0,0,0, 0,0,0,0,0,0, 0,0,0,0,0,0, 0,0,0,0,0,0};
//		double[] timeBreakRate = {0, 0,0,0,0,0,0, 0,0,0,0,0,0, 0,0,0,0,0,0, 0,0,0,0,0,0};
//		
//		for (Text value :values){
//			String []str = value.toString().trim().split(",");
//			timePlay[Integer.valueOf(str[0])] += 1;
//			if (Integer.valueOf(str[1])<=60){
//				timeBreak[Integer.valueOf(str[0])] +=1;
//			}
//		}
//		
//		int tempPlayAll = 0;
//		int tempBreakAll = 0;
//		for (int i=1;i<=24;i++){
//			tempPlayAll += timePlay[i];
//			tempBreakAll += timeBreak[i];
//			if(timePlay[i]>0){
//				timeBreakRate[i] = ((double)(1000*timeBreak[i]/timePlay[i]))/10;
//			}
//			else {
//				timeBreakRate[i] = 0;
//			}
//		}
//		timePlay[0] = tempPlayAll;
//		timeBreak[0] = tempBreakAll;
//		if (tempPlayAll>0){			
//			timeBreakRate[0] = ((double)(1000*tempBreakAll/tempPlayAll))/10;
//		}
//		else {
//			timeBreakRate[0] = 0;
//		}
//		
//		String strBR = "";
//		for (int i=0;i<=24;i++){
//			strBR += "," + String.valueOf(timeBreakRate[i]) + "%";
//			System.out.println("timeBreakRate"+i+"= "+timeBreakRate[i]);
//		}
//		keyText.set(key+strBR);
//		System.out.println("strBR= "+strBR);
//		valueInt.set("");
	}

}
