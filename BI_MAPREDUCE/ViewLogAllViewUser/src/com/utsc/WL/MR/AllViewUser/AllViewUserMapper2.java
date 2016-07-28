package com.utsc.WL.MR.AllViewUser;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AllViewUserMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// intput
		// KEY: VALUE:
		// DATE|AreaCode|HdFlag|HOUR|UserId 1
		// output
		// DATE|AreaCode|HdFlag|HOUR UserId
		if (value.getLength() > 0) {
			String[] str = value.toString().trim().split("\\|", -1);
			keyText.set(str[0] + "|" + str[1] + "|" + str[2] + "|" + str[3]);
			valueText.set(str[4]);
			context.write(keyText, valueText);
		}
	}
}
