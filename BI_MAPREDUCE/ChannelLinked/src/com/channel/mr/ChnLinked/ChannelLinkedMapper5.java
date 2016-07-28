package com.channel.mr.ChnLinked;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.viewlog.util.TexttransformGbktoUtf8;

public class ChannelLinkedMapper5 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// UID|ORDERDATE|STARTDATE|ENDDATE|BUYTYEP|PACKAGENAME/PROGRAMNAME|PACKAGECODE/PROGRAMCODE|
		// PRIZE|SERVICENAME|1|1.ORDER/0.CANCEL

		// UID|PACKAGECODE/PROGRAMCODE
		if (value.getLength() > 0) {
			Text line = TexttransformGbktoUtf8.transformTextToUTF8(value, "GBK");
			String[] str = line.toString().split("\\|", -1);
			if (str[10].trim().equals("1")) {
				if (str[6].trim().equals("null")) {

				} else {
					keyText.set(str[0]);
					valueText.set(str[6]);
					context.write(keyText, valueText);
				}
			}
		}
	}
}
