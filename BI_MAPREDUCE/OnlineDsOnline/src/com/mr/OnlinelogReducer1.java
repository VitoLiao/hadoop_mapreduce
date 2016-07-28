package com.mr;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.util.ViewlogDateUtil;

public class OnlinelogReducer1 extends Reducer<Text, Text, Text, NullWritable> {

	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		long timeSum = 0;

		for (Text value : values) {
			String[] str = value.toString().split(",");
			Date onlinelogStartDate;
			Date onlinelogEndDate;

			try {
				onlinelogStartDate = ViewlogDateUtil.str2date(str[0]);
				onlinelogEndDate = ViewlogDateUtil.str2date(str[1]);
				timeSum += (onlinelogEndDate.getTime() - onlinelogStartDate.getTime()) / 1000;
			} catch (ParseException e) {
				e.printStackTrace();
				return;
			}

		}

		String[] str = key.toString().split(",");

		keyText.set(str[0] + "|" + str[1] + "|" + timeSum + "|" + str[2]);

		context.write(keyText, NullWritable.get());
	}
}
