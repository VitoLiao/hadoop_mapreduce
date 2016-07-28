package com.mr;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LostUserReducer3 extends Reducer<Text, Text, Text, Text> {
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
	    if (k1[1]!=0) {
	    	float k10=k1[0];
	    	float k11=k1[1];
			lostuser = k10/k11;
			BigDecimal b=new BigDecimal(lostuser);
			float sortLostuser=b.setScale(3,BigDecimal.ROUND_HALF_UP).floatValue();
			valueText.set(String.valueOf(sortLostuser));//+"|"+k1[0]+"|"+k1[1]
			context.write(key, valueText);
		}
		
		//valueText.set(String.valueOf(k1[0])+"|"+String.valueOf(k1[1]));
		//context.write(key, valueText);
	}
}
