package com.utsc.WL.MR.PartViewUser;

import java.io.IOException;
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
			type = "9";
		}else if(keystr[3].equals("p")){
			type = "8";
		}
		String viewTimeAll = "0";
		String[] viewStr = {"0","0","0","0","0","0","0","0",
				"0","0","0","0","0","0","0","0",
				"0","0","0","0","0","0","0","0"};
		for(Text value : values){
			String[] str = value.toString().trim().split("\\|",-1);
			if(str[0].trim().equals("24")){
				viewTimeAll = str[1].trim();
			}else{
				viewStr[Integer.valueOf(str[0])] = str[1];
			}
		}
		keyText.set(keystr[0]+"|"+type+"|"+ viewTimeAll +"|"+String.join("|", viewStr));
		valueText.set(keystr[1]+"|"+keystr[2]);
		context.write(keyText, valueText);
	}
}
