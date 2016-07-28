package com.utsc.WL.MR.AllViewUser;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.viewlog.util.TexttransformGbktoUtf8;

public class AllViewUserMapper1 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// output
		// KEY: VALUE:
		// DATE|AreaCode|HdFlag|HOUR|UserId 1

		if (value.getLength() > 0) {
			Text line = TexttransformGbktoUtf8.transformTextToUTF8(value, "GBK");

			String[] str = line.toString().trim().split(",", -1);
			if (str.length >= 15) {
//				String Id = str[0].trim();
				String LogType = str[1].trim();
				String AreaCode = str[2].trim();
				String UserId = str[3].trim();
//				String MediaCode = str[4].trim();
				String StartTime = str[5].trim();
				String EndTime = str[6].trim();
				 String TimeInterval = str[7].trim();
//				String TimeInterval = String.valueOf(Math.round(Float.valueOf(str[7].trim())));
				String HdFlag = str[12].trim();
//				String ParentObjectCode = str[15].trim();

				String calDate = context.getConfiguration().get("calDate");
				String startDate = calDate.substring(0, 10).trim();

				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

				if (LogType.equals("c") | LogType.equals("t") | LogType.equals("v") | LogType.equals("p")
						| LogType.equals("s")) {

					long DefaultStartTime = 0;
					long DefaultEndTime = 0;
					long LStartTime = 0;
					long LEndTime = 0;
					try {
						DefaultStartTime = sdf.parse(startDate + " 00:00:00").getTime();
						DefaultEndTime = sdf.parse(startDate + " 23:59:59").getTime();
						LStartTime = sdf.parse(StartTime).getTime();
						LEndTime = sdf.parse(EndTime).getTime();
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					String hourStart = "00";
					String hourEnd = "00";

					if (Integer.valueOf(TimeInterval) > 0) {
						startDate += " 00:00:00.0";
						if (LEndTime < DefaultStartTime) {

						} else if (LStartTime > DefaultEndTime) {

						} else {
							keyText.set(startDate + "|" + AreaCode + "|" + HdFlag + "|" + "24" + "|" + UserId);
							valueText.set("1");
							context.write(keyText, valueText);
						}

						if (LStartTime < DefaultStartTime && LEndTime > DefaultEndTime) {
							hourStart = "00";
							hourEnd = "23";
							for (int h = Integer.valueOf(hourStart); h <= Integer.valueOf(hourEnd); h++) {
								keyText.set(startDate + "|" + AreaCode + "|" + HdFlag + "|" + String.valueOf(h) + "|"
										+ UserId);
								valueText.set("1");
								context.write(keyText, valueText);
							}

						} else if (LStartTime < DefaultStartTime && LEndTime > DefaultStartTime
								&& LEndTime <= DefaultEndTime) {
							hourStart = "00";
							hourEnd = EndTime.trim().substring(11, 13);
							for (int h = Integer.valueOf(hourStart); h <= Integer.valueOf(hourEnd); h++) {
								keyText.set(startDate + "|" + AreaCode + "|" + HdFlag + "|" + String.valueOf(h) + "|"
										+ UserId);
								valueText.set("1");
								context.write(keyText, valueText);
							}

						} else if (LStartTime >= DefaultStartTime && LEndTime <= DefaultEndTime
								&& LEndTime > DefaultStartTime) {
							hourStart = StartTime.trim().substring(11, 13);
							hourEnd = EndTime.trim().substring(11, 13);
							for (int h = Integer.valueOf(hourStart); h <= Integer.valueOf(hourEnd); h++) {
								keyText.set(startDate + "|" + AreaCode + "|" + HdFlag + "|" + String.valueOf(h) + "|"
										+ UserId);
								valueText.set("1");
								context.write(keyText, valueText);
							}

						} else if (LStartTime >= DefaultStartTime && LStartTime < DefaultEndTime
								&& LEndTime > DefaultEndTime) {
							hourStart = StartTime.trim().substring(11, 13);
							hourEnd = "23";
							for (int h = Integer.valueOf(hourStart); h <= Integer.valueOf(hourEnd); h++) {
								keyText.set(startDate + "|" + AreaCode + "|" + HdFlag + "|" + String.valueOf(h) + "|"
										+ UserId);
								valueText.set("1");
								context.write(keyText, valueText);
							}
						}
					}
				}
			}
		}
	}
}