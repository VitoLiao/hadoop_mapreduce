package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserViewAreaReducer3 extends Reducer<Text, Text, Text, Text>{
	
	Text valueText = new Text();
	private Text keyText = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String userAll = "";
		String[] valueTemp = null;
		for(Text value : values){
			System.out.println("value= "+value);
			String[] str = value.toString().trim().split("\\|",-1);
//			System.out.println("value= "+value);
//			System.out.println("size= "+str.length);
			if(str.length<2){
				userAll = value.toString();
			}else{
				valueTemp = str;
			}
		}
//		System.out.println("userall= "+userAll);
		keyText.set(valueTemp[0]+"|"+valueTemp[1]+"|"+valueTemp[2]+"|"+valueTemp[3]+"|"
				+valueTemp[4]+"|"+userAll+"|"+valueTemp[6]+"|"
				+valueTemp[7]+"|"+valueTemp[8]);
		valueText.set(valueTemp[9]+"|"+valueTemp[10]+"|"+valueTemp[11]);
		context.write(keyText, valueText);
//		System.out.println("key "+keyText);
//		System.out.println("value "+valueText);
	}

}
