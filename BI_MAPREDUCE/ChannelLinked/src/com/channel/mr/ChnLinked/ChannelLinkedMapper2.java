package com.channel.mr.ChnLinked;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.viewlog.util.TexttransformGbktoUtf8;

public class ChannelLinkedMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// input
		// ID|STARTTIME|STOPTIME|MEDIACODE|MEDIANAME|CHANNELCODE|CHANNELNAME
		// output
		// CHANNELCODE
		// MEDIACODE|STARTTIME|STOPTIME
		if (value.getLength() > 0) {
			Text line = TexttransformGbktoUtf8.transformTextToUTF8(value, "GBK");
			String[] str = line.toString().split("\\|", -1);
			if (str.length >= 6) {
				String calDate = context.getConfiguration().get("calDate");
				List<String> NeedcalDate = Arrays.asList(calDate.split("##", -1));
				String startDate = str[1].substring(0, 10);
				if (NeedcalDate.contains(startDate)) {
					// if
					// (str[5].trim().equals("02000000000000000000000000000263"))
					// {
					if (str[3].trim().equals("null")) {

					} else {
						if (str[5].trim().equals("null")) {

						} else {
							keyText.set(str[5]);
							valueText.set(str[3] + "|" + str[1] + "|" + str[2]);
							context.write(keyText, valueText);
						}
						// }
					}
				}
			}
		}
	}
}
