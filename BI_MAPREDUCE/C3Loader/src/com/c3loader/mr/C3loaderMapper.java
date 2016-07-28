package com.c3loader.mr;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.util.ViewlogDateUtil;
import com.util.ViewlogOptionUtil;
import com.util.ViewlogRowData;
import com.util.ViewlogRowDataSplit;
import com.util.ViewlogStringUtil;

public class C3loaderMapper extends Mapper<LongWritable, Text, Text, Text> {

	Text keyText = new Text();
	Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		if (value.getLength() > 0) {

			Text line = ViewlogStringUtil.transform(value, "GBK");

			ViewlogRowData viewlogRowData = null;
			try {
				viewlogRowData = new ViewlogRowData(line.toString(), ",");
			} catch (Exception e) {
				System.out.println(e.getMessage());
				return;
			}
			
			String dateType = context.getConfiguration().get("DateType");

			ArrayList<ViewlogRowData> dataArray = null;

			try {
				dataArray = ViewlogRowDataSplit.split(viewlogRowData);
			} catch (ParseException e) {
				e.printStackTrace();
				System.out.println("Parse error, line : " + line.toString());
				return;
			}

			for (int i = 0; i < dataArray.size(); i++) {
				ViewlogRowData tmpData = dataArray.get(i);

				DayInfomation info = null;
				try {
					info = calcDayViewDuration(ViewlogDateUtil.str2date(tmpData.getStartTime()), ViewlogDateUtil.str2date(tmpData.getEndTime()));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					continue;
				}

				boolean isNeedCalc = false;
				String startDate = null;

				startDate = context.getConfiguration().get("CalcDate");
				
				isNeedCalc = ViewlogOptionUtil.isNeedCalc(startDate, ViewlogDateUtil.getDate(tmpData.getStartTime()), dateType);
				
				if (isNeedCalc) {
					startDate += " 00:00:00.0";
					keyText.set(startDate + "|" + tmpData.getAreaCode() + "|" + tmpData.getMediaCode() + "|"
							+ tmpData.getLogType() + "|" + "unknow" + "|" + tmpData.getHdFlag() + "|" + "0" + "|" + "unknow");
					valueText.set(tmpData.getUserId() + "|" + info.viewDurationToString() + "|"
							+ info.getViewDurationGt5Min());

					context.write(keyText, valueText);
				}
			}

		}

	}

	/**
	 * 将输入的startDate和endDate进行24小时拆分，并返回DayInfomation的对象作为结果
	 * */
	public DayInfomation calcDayViewDuration(Date startDate, Date endDate) throws ParseException {

		DayInfomation info = new DayInfomation();

		int startHour = ViewlogDateUtil.getHourOfDay(startDate);
		int endHour = ViewlogDateUtil.getHourOfDay(endDate);

		int[] viewDuration = new int[DayInfomation.HOUR_COUNT];

		for (int i = 0; i < viewDuration.length; i++) {
			viewDuration[i] = 0;
		}

		int viewTotalDuration = (int) ViewlogRowDataSplit.getDateTimeoffset(startDate, endDate);

		int viewDurationGt5Min = 0;

		if (viewTotalDuration > 5 * 60) {
			viewDurationGt5Min = viewTotalDuration;
		}

		if (startHour == endHour) {
			viewDuration[startHour] = viewTotalDuration;
		}
		else {
			int startMin = ViewlogDateUtil.getMinuteOfDay(startDate);
			int startSec = ViewlogDateUtil.getSecondOfDay(startDate);
			int endMin = ViewlogDateUtil.getMinuteOfDay(endDate);
			int endSec = ViewlogDateUtil.getSecondOfDay(endDate);

			viewDuration[startHour] = 3600 - (startMin * 60 + startSec);
			viewDuration[endHour] = endMin * 60 + endSec;

			for (int i = startHour + 1, j = 1; j < endHour - startHour; i++, j++) {
				viewDuration[i] = 3600;
			}
		}

		info.setViewTotalDuration(viewTotalDuration);
		info.setViewDuration(viewDuration);
		info.setViewDurationGt5Min(viewDurationGt5Min);

		return info;
	}
}
