package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChannelTimeReducer2 extends Reducer<Text, IntWritable, Text, NullWritable> {
	private Text valueInt = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int usersCount = 0;

		for (IntWritable value : values) {
			usersCount += value.get();
		}

		String[] str = key.toString().split("\\|");

		String sday = str[0];
		String channelCode = str[1];
		String areaCode = str[2];
		String serviceId = str[3];
		String hdflag = str[4];
		String dateType = str[5];
		String vct = str[6];

		valueInt.set(String.valueOf(usersCount));

		keyText.set(channelCode + "|" + areaCode + "|" + usersCount + "|" + vct + "|" + sday + "|" + serviceId + "|"
				+ hdflag + "|" + dateType);
		context.write(keyText, NullWritable.get());
	}
}
