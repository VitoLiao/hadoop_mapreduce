package com.mr;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.util.ViewlogDateUtil;
import com.util.ViewlogOptionUtil;
import com.util.ViewlogRowData;
import com.util.ViewlogRowDataSplit;
import com.util.ViewlogStringUtil;

public class ChannelLinkedMapper1 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// 鍙鐞嗗唴瀹逛笉涓虹┖鐨勬暟鎹�
		if (value.getLength() > 0) {

			// 灏嗘暟鎹浆鐮佹垚GBK妯″紡
			Text line = ViewlogStringUtil.transform(value, "GBK");

			// 浠庤鍐呭涓幏寰梫iewlog鏁版嵁瀵硅薄
			ViewlogRowData viewlogRowData = null;

			try {
				viewlogRowData = new ViewlogRowData(line.toString(), ",");
			} catch (Exception e) {
				System.out.println(e.getMessage());
				return;
			}

			// 鍒涘缓viewlog鏁版嵁瀵硅薄鏁扮粍锛岀敤浜庡瓨鏀惧垏鍒嗗嚭鐨勫涓獀iewlog鏁版嵁瀵硅薄
			ArrayList<ViewlogRowData> dataArray = null;

			// 灏唙iewlog鏁版嵁瀵硅薄杩涜鍒囧垎
			try {
				dataArray = ViewlogRowDataSplit.split(viewlogRowData);
			} catch (ParseException e) {
				e.printStackTrace();
				System.out.println("Parse error, line : " + line.toString());
				return;
			}

			// 鍙鐞嗚鐪嬬洿鎾殑viewlog鏁版嵁
			if (viewlogRowData.getLogType().equals("c")) {
				// 閬嶅巻鏁扮粍涓墍鏈夌殑viewlog鏁版嵁瀵硅薄
				for (int i = 0; i < dataArray.size(); i++) {
					ViewlogRowData tmpData = dataArray.get(i);

					String startDate = null;
					String dateType = context.getConfiguration().get("DateType");

					boolean isNeedCalc = false;

					startDate = context.getConfiguration().get("CalcDate");
					
					isNeedCalc = ViewlogOptionUtil.isNeedCalc(startDate, ViewlogDateUtil.getDate(tmpData.getStartTime()), dateType);
					
					// ֻ������Ҫ����������ڣ��������ɲ���"-s"����(-s Ĭ��Ϊ����)
					if (isNeedCalc) {
						// sqoop鍙敮鎸佸叏鏍煎紡鐨勬棩鏈�
						startDate += " 00:00:00.0";
						// KEY: VALUE:
						// 鏃ユ湡|CONTENTTYPE|TYPE|CODE|USERID TimeInterval
						if (Integer.valueOf(tmpData.getTimeInterval()) > 0) {
							keyText.set(startDate + "|" + "c" + "|" + ViewlogOptionUtil.CalcType.valueOf(dateType) + "|"
									+ tmpData.getMediaCode() + "|" + tmpData.getUserId());
							valueText.set(String.valueOf(tmpData.getTimeInterval()));
							context.write(keyText, valueText);
						}
					}

				}
			} // t and ct
			else if (viewlogRowData.getLogType().equals("t")) {
				for (int i = 0; i < dataArray.size(); i++) {
					ViewlogRowData tmpData = dataArray.get(i);

					boolean isNeedCalc = false;

					String startDate = context.getConfiguration().get("CalcDate");
					String dateType = context.getConfiguration().get("DateType");
					
					isNeedCalc = ViewlogOptionUtil.isNeedCalc(startDate, ViewlogDateUtil.getDate(tmpData.getStartTime()), dateType);
					
					// ֻ������Ҫ����������ڣ��������ɲ���"-s"����(-s Ĭ��Ϊ����)
					if (isNeedCalc) {
						startDate += " 00:00:00.0";
						// t
						if (Integer.valueOf(tmpData.getTimeInterval()) > 0) {
							keyText.set(startDate + "|" + "t" + "|" + ViewlogOptionUtil.CalcType.valueOf(dateType) + "|"
									+ tmpData.getMediaCode() + "|" + tmpData.getUserId());
							valueText.set(String.valueOf(tmpData.getTimeInterval()));
							context.write(keyText, valueText);
						}
						// ct
						if (Integer.valueOf(tmpData.getTimeInterval()) > 0) {
							keyText.set(startDate + "|" + "ct" + "|" + ViewlogOptionUtil.CalcType.valueOf(dateType)
									+ "|" + tmpData.getParentObjectCode() + "|" + tmpData.getUserId());
							valueText.set(String.valueOf(tmpData.getTimeInterval()));
							context.write(keyText, valueText);
						}
					}
				}
			}
		}
	}
}
