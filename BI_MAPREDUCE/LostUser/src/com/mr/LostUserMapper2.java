package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LostUserMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	Text keyText = new Text();
	Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// ֻ�������ݲ�Ϊ�յ�����
		if (value.getLength() > 0) {
			String[] str = value.toString().trim().split("\\|");
			// System.out.println(str[0]+str[1]+str[2]+str[3]+str[4]+str[5]+str[6]);
			String getDate = context.getConfiguration().get("CalcDate");
			if (str[0].length() < 10) {
				System.out.println(str[0] + str[1] + str[2] + str[3] + str[4] + str[5] + str[6]);
			}
			if (str[0].substring(8, 10).equals(getDate.substring(6, 8))) {
				// ����������������Ӧ����
				keyText.set(str[1] + "|" + str[2] + "|" + str[3] + "|" + str[4] + "|" + str[5]);
				valueText.set("1" + "|" + "0");
				context.write(keyText, valueText);
			}
			// ����������������Ӧ��ǰһ��
			else {
				int usetime = Integer.parseInt(str[6]);
				if (usetime >= 600) {

					keyText.set(str[1] + "|" + str[2] + "|" + str[3] + "|" + str[4] + "|" + str[5]);
					valueText.set("2" + "|" + "1");
					context.write(keyText, valueText);
				} else {

					keyText.set(str[1] + "|" + str[2] + "|" + str[3] + "|" + str[4] + "|" + str[5]);
					valueText.set("0" + "|" + "1");
					context.write(keyText, valueText);
				}
			}
 //value1��Ӻ��ʾ��ͬ�ĺ��� ������Ϊ2����ʾǰ����������10���ӣ�������û���ĵ��û�
			// keyΪmonthday + channelcode + areacode + hdflag + datetype +
			// clicktimes

			// context.write(keyText, valueText);
		}
	}
}
