package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LostUsercombin3 extends Reducer<Text, Text, Text, Text> {
	Text valueText = new Text();
	Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int[] k1 = { 0, 0 };
		double lostuser = 0;
		// ArrayList list=new ArrayList();

		for (Text value : values) {
			String[] str = value.toString().split("\\|");

			int intstr0 = Integer.parseInt(str[0]);
			int intstr1 = Integer.parseInt(str[1]);
			k1[0] = intstr0 + k1[0];
			k1[1] = intstr1 + k1[1];
		}
	  
			valueText.set(k1[0]+"|"+k1[1]);//+"|"+k1[0]+"|"+k1[1]
			context.write(key, valueText);
		
		
		//valueText.set(String.valueOf(k1[0])+"|"+String.valueOf(k1[1]));
		//context.write(key, valueText);
	}
}
