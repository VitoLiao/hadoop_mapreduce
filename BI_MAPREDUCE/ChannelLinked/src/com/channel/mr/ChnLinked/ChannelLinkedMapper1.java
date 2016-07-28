package com.channel.mr.ChnLinked;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.viewlog.util.TexttransformGbktoUtf8;

public class ChannelLinkedMapper1 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// System.out.println("M1");
		if (value.getLength() > 0) {

			Text line = TexttransformGbktoUtf8.transformTextToUTF8(value, "GBK");

			String[] str = line.toString().trim().split(",", -1);
			if (str.length >= 16) {
				// String Id = str[0].trim();
				String LogType = str[1].trim();
				// String AreaCode = str[2].trim();
				String UserId = str[3].trim();
				String MediaCode = str[4].trim();
				String StartTime = str[5].trim();
				String EndTime = str[6].trim();
				String TimeInterval = str[7].trim();
				// String HdFlag = str[12].trim();
				String ParentObjectCode = str[15].trim();

				String calType = context.getConfiguration().get("calType");
				String calDate = context.getConfiguration().get("calDate");
				List<String> NeedcalDate = Arrays.asList(calDate.split("##", -1));
				String startDate = StartTime.substring(0, 10);
				if (NeedcalDate.contains(startDate)) {
					if (Integer.valueOf(TimeInterval) > 0) {
						startDate += " 00:00:00.0";
						if (LogType.equals("c")) {

							// KEY: VALUE:
							// c DATE|CONTENTTYPE|TYPE|CODE|USERID|TIMEINTERVAL

							if (ParentObjectCode.trim().length() == 0) {

							} else {
								// keyText.set("c");
								// valueText.set(startDate + "|" + LogType + "|"
								// + calType + "|" + ParentObjectCode + "|"
								// + UserId + "|" + TimeInterval);
								// context.write(keyText, valueText);
								// if (Integer.valueOf(TimeInterval) > 7200) {
								if (ParentObjectCode.trim().equals("null")) {
								} else {
									keyText.set("c" + "|" + startDate + "|" + LogType + "|" + calType + "|"
											+ ParentObjectCode + "|" + UserId);
									valueText.set(TimeInterval);
									context.write(keyText, valueText);
								}
								// }
								// ot
								// DATE|CONTENTTYPE|TYPE|CODE|USERID|TIMEINTERVAL|STARTTIME|ENDTIME
								// if
								// (ParentObjectCode.equals("02000000000000000000000000000263"))
								// {
								if (ParentObjectCode.trim().equals("null")) {
								} else {
									keyText.set("ot" + "|" + startDate + "|" + "ot" + "|" + calType + "|"
											+ ParentObjectCode + "|" + UserId);
									valueText.set(TimeInterval + "|" + StartTime + "|" + EndTime);
									context.write(keyText, valueText);
								}
								// }
							}
						} // t and ct
						else if (LogType.equals("t")) {
							// t
							// t DATE|CONTENTTYPE|TYPE|CODE|USERID|TIMEINTERVAL
							if (MediaCode.trim().length() == 0) {

							} else {
								// if (Integer.valueOf(TimeInterval) > 7200) {
								if (MediaCode.trim().equals("null")) {
								} else {
									keyText.set("t" + "|" + startDate + "|" + "t" + "|" + calType + "|" + MediaCode
											+ "|" + UserId);
									valueText.set(TimeInterval);
									context.write(keyText, valueText);
								}
								// }
							}
							// ct
							// ct DATE|CONTENTTYPE|TYPE|CODE|USERID|TIMEINTERVAL
							if (ParentObjectCode.trim().length() == 0) {

							} else {
								// if (Integer.valueOf(TimeInterval) > 7200) {
								if (ParentObjectCode.trim().equals("null")) {
								} else {
									keyText.set("ct" + "|" + startDate + "|" + "ct" + "|" + calType + "|"
											+ ParentObjectCode + "|" + UserId);
									valueText.set(TimeInterval);
									context.write(keyText, valueText);
								}
								// }
							}
						}
					}
				}
			}
		}
	}
}
