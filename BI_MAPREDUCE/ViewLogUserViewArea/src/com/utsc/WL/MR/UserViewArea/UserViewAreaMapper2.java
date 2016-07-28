package com.utsc.WL.MR.UserViewArea;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserViewAreaMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// input
		// key value
		// a|DATE|AreaCode|LogType|viewInterval
		// b|DATE|AreaCode|UserId

		// a|DATE|AreaCode|LogType	viewInterval
		// b|DATE|AreaCode	UserId
		if (value.getLength() > 0) {
			String[] str = value.toString().trim().split("\\|", -1);
			if(str[0].trim().equals("a")){
				keyText.set(str[0]);
				valueText.set(str[1]+"|"+str[2]+"|"+str[3]+"|"+str[4]);
				context.write(keyText, valueText);
			}else if (str[0].trim().equals("b")) {
				keyText.set(str[0]+"|"+str[1]+"|"+str[2]+"|"+str[3]);
				valueText.set(str[4]);
				context.write(keyText, valueText);
			}else if (str[0].trim().equals("c")) {
				keyText.set(str[0]);
				valueText.set(str[1]+"|"+str[2]+"|"+str[3]);
				context.write(keyText, valueText);
			}else if (str[0].trim().equals("d")) {
				keyText.set(str[0]+"|"+str[1]+"|"+str[2]);
				valueText.set(str[3]);
				context.write(keyText, valueText);
			}else if (str[0].trim().equals("e")) {
				keyText.set(str[0]+"|"+str[1]+"|"+str[2]);
				valueText.set(str[3]);
				context.write(keyText, valueText);
			}
		}
	}
}
