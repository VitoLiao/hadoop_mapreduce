package com.mr;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AllViewUserReducer3 extends Reducer<Text, Text, Text, Text>{
	
	Text valueText = new Text();
	private Text keyText = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// intput
		// KEY: VALUE:
		// DATE|AreaCode|HdFlag	HOUR|viewTime
		// output
		// DATE|AreaCode|HdFlag	HOUR|viewTime
		String[] keystr = key.toString().trim().split("\\|",-1);
		String aa = "";
		long viewTimeAll = 0;
		String[] viewStr = {"0","0","0","0","0","0","0","0",
				"0","0","0","0","0","0","0","0",
				"0","0","0","0","0","0","0","0"};
		for(Text value : values){
//			System.out.println("value= "+value);
//			System.out.println("s= "+value.toString().substring(0,1));
			String[] str = value.toString().trim().split("\\|",-1);
//			System.out.println(str.length);
			if(str.length<2){
				aa = value.toString();
			}else {
				viewTimeAll += Integer.valueOf(str[1]);
				viewStr[Integer.valueOf(str[0])] = str[1];
//				System.out.println(Integer.valueOf(str[0])+", "+str[1]);
			}
		}
		BigInteger bivta = BigInteger.valueOf(viewTimeAll);
		keyText.set(keystr[0]+"|"+aa+"|"+String.join("|", viewStr));
		valueText.set(keystr[1]+"|"+keystr[2]);
		context.write(keyText, valueText);
	}
}
