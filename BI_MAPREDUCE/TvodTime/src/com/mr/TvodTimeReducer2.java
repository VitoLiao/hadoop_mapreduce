package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
* @author VitoLiao
* @version 2016��3��3�� ����3:33:07
*/
public class TvodTimeReducer2 extends Reducer<Text, IntWritable, Text, NullWritable>{
	private Text keyText = new Text();
	private Text valueInt = new Text();
	public void reduce(Text key, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException {
		int usersCount = 0;
		
		for (IntWritable value : values) {
			usersCount += value.get();
		}
		
		String []str = key.toString().split("\\|");
		
		String sday = str[0];
		String cc = str[1];
		String sc = str[2];
		String ac = str[3];
		String hdflag = str[4];
		String vct = str[5];
		
		valueInt.set(String.valueOf(usersCount));
		keyText.set(cc + "|" + sc + "|" + ac + "|" + valueInt + "|" + vct + "|" + sday + "|" + hdflag);
		
		context.write(keyText, NullWritable.get());
	}
}
