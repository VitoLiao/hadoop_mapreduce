package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LostUserMapper3 extends Mapper<LongWritable, Text, Text, Text> {

	Text keyText = new Text();
	Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// ֻ�������ݲ�Ϊ�յ�����
		if (value.getLength() > 0) {
			String[] str = value.toString().trim().split("\\|");

			String getDate = context.getConfiguration().get("CalcDate");
					/*.substring(0, 4) + "-"
					+ context.getConfiguration().get("CalcDate").substring(4, 6) + "-"
					+ context.getConfiguration().get("CalcDate").substring(6, 8);*/
			keyText.set(getDate +" 00:00:00.0"+ "|" + str[0] + "|" + "D" + "|" + str[1] + "|" + str[2] + "|" + str[3]);
			valueText.set(str[5] + "|" + str[6]);
			context.write(keyText, valueText);

			// keyΪmonthday + channelcode + areacode + hdflag + datetype +
			// clicktimes

			// context.write(keyText, valueText);
		}
	}
}
