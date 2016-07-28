package com.utsc.WL.MR.UserViewArea;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.viewlog.util.TexttransformGbktoUtf8;

public class UserViewAreaMapper1 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// output
		// KEY: VALUE:
		// a|DATE|AreaCode|LogType TimeInterval or b|DATE|AreaCode|UserId 1

		if (value.getLength() > 0) {
			Text line = TexttransformGbktoUtf8.transformTextToUTF8(value, "GBK");
			// System.out.println("M1 " + line);
			// case 0: this.setId(value);
			// case 1: this.setLogType(value);
			// case 2: this.setAreaCode(value);
			// case 3: this.setUserId(value);
			// case 4: this.setMediaCode(value);
			// case 5: this.setStartTime(value);
			// case 6: this.setEndTime(value);
			// case 7: this.setTimeInterval(Integer.parseInt(value));
			// case 8:
			// case 9:
			// case 10:
			// case 11:
			// case 12: this.setHdFlag(value);
			// case 13:
			// case 14:
			// case 15: this.setParentObjectCode(value);

			String[] str = line.toString().trim().split(",", -1);
			if (str.length >= 15) {
				String Id = str[0].trim();
				String LogType = str[1].trim();
				String AreaCode = str[2].trim();
				String UserId = str[3].trim();
				String MediaCode = str[4].trim();
				String StartTime = str[5].trim();
				String EndTime = str[6].trim();
				 String TimeInterval = str[7].trim();
//				 String.valueOf(Math.round(Float.valueOf(str[7].trim())));
//				String TimeInterval = String.valueOf(Math.round(Math.floor(Float.valueOf(str[7].trim()))));
				String HdFlag = str[12].trim();
				String ParentObjectCode = str[15].trim();

				String calDate = context.getConfiguration().get("calDate");

				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

				String startDate = calDate.substring(0, 10).trim();

				if (LogType.equals("c") || LogType.equals("t") || LogType.equals("v") || LogType.equals("s")
						|| LogType.equals("p")) {
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

					// a : seperate view timeinterval
					// b : seperate user number
					// c : all view timeinterval
					// d : all disctint user number
					// e : all view number

					startDate += " 00:00:00.0";
					if (LStartTime < DefaultStartTime && LEndTime < DefaultStartTime) {

					} else if (LEndTime > DefaultEndTime && LStartTime > DefaultEndTime) {

					} else {

						keyText.set("b" + "|" + startDate + "|" + AreaCode + "|" + LogType);
						valueText.set("1");
						context.write(keyText, valueText);

						keyText.set("d" + "|" + startDate + "|" + AreaCode + "|" + UserId);
						valueText.set("1");
						context.write(keyText, valueText);

						keyText.set("e" + "|" + startDate + "|" + AreaCode);
						valueText.set("1");
						context.write(keyText, valueText);
					}

					if (Integer.valueOf(TimeInterval) > 0) {
						if (LStartTime < DefaultStartTime && LEndTime > DefaultEndTime) {
							keyText.set("a" + "|" + startDate + "|" + AreaCode + "|" + LogType);
							valueText.set("86399");// seconds in one day
							context.write(keyText, valueText);

							keyText.set("c" + "|" + startDate + "|" + AreaCode);
							valueText.set("86399");// seconds in one day
							context.write(keyText, valueText);

						} else if (LStartTime < DefaultStartTime && LEndTime > DefaultStartTime
								&& LEndTime <= DefaultEndTime) {
							keyText.set("a" + "|" + startDate + "|" + AreaCode + "|" + LogType);
							valueText.set(String.valueOf((LEndTime - DefaultStartTime) / 1000 - 1));
							context.write(keyText, valueText);

							keyText.set("c" + "|" + startDate + "|" + AreaCode);
							valueText.set(String.valueOf((LEndTime - DefaultStartTime) / 1000 - 1));
							context.write(keyText, valueText);

						} else if (LStartTime >= DefaultStartTime && LEndTime <= DefaultEndTime
								&& LEndTime > DefaultStartTime) {
							keyText.set("a" + "|" + startDate + "|" + AreaCode + "|" + LogType);
							valueText.set(String.valueOf(TimeInterval));
							context.write(keyText, valueText);

							keyText.set("c" + "|" + startDate + "|" + AreaCode);
							valueText.set(String.valueOf(TimeInterval));
							context.write(keyText, valueText);

						} else if (LStartTime >= DefaultStartTime && LStartTime < DefaultEndTime
								&& LEndTime > DefaultEndTime) {
							keyText.set("a" + "|" + startDate + "|" + AreaCode + "|" + LogType);
							valueText.set(String.valueOf((DefaultEndTime - LStartTime) / 1000 - 1));
							context.write(keyText, valueText);

							keyText.set("c" + "|" + startDate + "|" + AreaCode);
							valueText.set(String.valueOf((DefaultEndTime - LStartTime) / 1000 - 1));
							context.write(keyText, valueText);

						}
					}
				}
			}
		}
	}
}