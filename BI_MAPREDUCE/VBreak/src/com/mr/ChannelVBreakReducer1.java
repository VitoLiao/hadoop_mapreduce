package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChannelVBreakReducer1 extends Reducer<Text, Text, Text, Text>{
	
	Text valueText = new Text();
	private Text keyText = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		//value�ĸ�ʽ��|BAll|B1|B2|B3|B4|B5|B6|B7|B8|B9|B10|B11|B12|B13|B14|B15|B16|B17|B18|B19|B20|B21|B22|B23|B24
		int[] timePlay = {0, 0,0,0,0,0,0, 0,0,0,0,0,0, 0,0,0,0,0,0, 0,0,0,0,0,0};
		int[] timeBreak = {0, 0,0,0,0,0,0, 0,0,0,0,0,0, 0,0,0,0,0,0, 0,0,0,0,0,0};
		double[] timeBreakRate = {0, 0,0,0,0,0,0, 0,0,0,0,0,0, 0,0,0,0,0,0, 0,0,0,0,0,0};
		
		for (Text value :values){
			String []str = value.toString().trim().split("\\|");
			timePlay[Integer.valueOf(str[0])] += 1;
			if (Integer.valueOf(str[1])<=60){
				timeBreak[Integer.valueOf(str[0])] +=1;
			}
		}
		
		int tempPlayAll = 0;
		int tempBreakAll = 0;
		for (int i=1;i<=24;i++){
			tempPlayAll += timePlay[i];
			tempBreakAll += timeBreak[i];
			if(timePlay[i]>0){
				timeBreakRate[i] = ((double)(10000*timeBreak[i]/timePlay[i]))/10000;
			}
			else {
				timeBreakRate[i] = 0;
			}
		}
		timePlay[0] = tempPlayAll;
		timeBreak[0] = tempBreakAll;
		if (tempPlayAll>0){			
			timeBreakRate[0] = ((double)(10000*tempBreakAll/tempPlayAll))/10000;
		}
		else {
			timeBreakRate[0] = 0;
		}
		
		String strBR = "";
		for (int i=0;i<=24;i++){
			strBR += String.valueOf(timeBreakRate[i]);
			System.out.println("timeBreakRate"+i+"= "+timeBreakRate[i]);
			if (i != 24) {
				strBR += "|";
			}
		}
//		keyText.set(key+strBR);
		keyText.set(key);
		System.out.println("strBR= "+strBR);
		valueText.set(strBR);
		
		context.write(keyText, valueText);
//		System.out.println("keytext= " +keyText);
		
	}
}

