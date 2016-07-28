package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OnlinelogReducer2 extends Reducer<Text, IntWritable, Text, NullWritable> {

	private Text keyText = new Text();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		long countSum = 0;

		for (IntWritable value : values) {
			countSum += value.get();
		}

		String []str = key.toString().split("\\|");
		String calcDay = str[0];
		String areaCode = str[1];
		String hdFlag = str[2];
		String type = str[3];
		
		keyText.set(calcDay + "|" + areaCode + "|" + hdFlag + "|" + countSum + "|" + type);
		context.write(keyText, NullWritable.get());
	}
}
