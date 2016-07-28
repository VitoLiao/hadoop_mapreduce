package com.mr;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PartViewUserReducer3 extends Reducer<Text, Text, Text, Text>{
	
	Text valueText = new Text();
	private Text keyText = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// intput
		// KEY: VALUE:
		// DATE|AreaCode|HdFlag|LogType 	HOUR|viewTime
		// output
		// DATE|AreaCode|HdFlag|LogType	HOUR|viewTime
		String[] keystr = key.toString().trim().split("\\|",-1);
		String type = "";
		if(keystr[3].equals("v")){
			type = "1";
		}else if(keystr[3].equals("c")){
			type = "2";
		}else if(keystr[3].equals("t")){
			type = "3";
		}else if(keystr[3].equals("s")){
			type = "8";
		}else if(keystr[3].equals("p")){
			type = "9";
		}
		long viewTimeAll = 0;
		String[] viewStr = {"0","0","0","0","0","0","0","0",
				"0","0","0","0","0","0","0","0",
				"0","0","0","0","0","0","0","0"};
		for(Text value : values){
//			System.out.println("value= "+value);
			String[] str = value.toString().trim().split("\\|",-1);
			if(str.length==2){
				viewStr[Integer.valueOf(str[0])] = str[1];
			}else if(str.length==1){
				viewTimeAll = Long.valueOf(value.toString());
			}
		}
		BigInteger bivta = BigInteger.valueOf(viewTimeAll);
		keyText.set(keystr[0]+"|"+type+"|"+bivta+"|"+String.join("|", viewStr));
		valueText.set(keystr[1]+"|"+keystr[2]);
		context.write(keyText, valueText);
	}
}
