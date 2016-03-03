package com.schedule.mr.time;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.schedule.dataobj.ScheduleRowData;
import com.viewlog.dataobj.ViewlogRowData;
import com.viewlog.dataobj.ViewlogRowDataSplit;
import com.viewlog.util.ViewlogDateUtil;
import com.viewlog.util.ViewlogOptionUtil;
import com.viewlog.util.ViewlogStringUtil;

/**
 * @author VitoLiao
 * @version 2016年2月29日 上午9:00:32
 */
public class ScheduleTimeMapper1 extends Mapper<LongWritable, Text, Text, Text> {
	private Text keyText = new Text();
	private Text valueText = new Text();
	private final static String scheduleFileName = "schedule";
	private final static String viewlogFileName = "viewlog";
	private final static String scheduleFileFlag = "s";
	private final static String viewlogFileFlag = "v";

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 只处理内容不为空的数据
		if (value.getLength() > 0) {

			String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();

			// 获得配置参数
			String startDateStr = null;
			String dateType = context.getConfiguration().get("DateType");
			int dateNum = Integer.parseInt(context.getConfiguration().get("DayNum"));

			// 将数据转码成GBK模式
			Text line = ViewlogStringUtil.transform(value, "GBK");

			// 创建schedule和viewlog的数据对象
			ScheduleRowData scheduleRowData = null;
			ViewlogRowData viewlogRowData = null;

			if (filePath.contains(scheduleFileName)) {
				scheduleRowData = new ScheduleRowData(line.toString(), "\\|");

				startDateStr = context.getConfiguration().get("CalcDate");

				try {
					Date scheduleStartDate = ViewlogDateUtil.str2date(context.getConfiguration().get("ScheduleStartDate"), "yyyy-MM-dd");
					Date scheduleEndDate = ViewlogDateUtil.str2date(context.getConfiguration().get("ScheduleEndDate"), "yyyy-MM-dd");

					if (ViewlogDateUtil.isInZone(ViewlogDateUtil.str2date(scheduleRowData.getStartTime()),
							scheduleStartDate, scheduleEndDate)) {

						keyText.set(scheduleRowData.getChannelCode());
						valueText.set(scheduleFileFlag + "|" + scheduleRowData.getScheduleCode() + "|"
								+ scheduleRowData.getStartTime() + "|" + scheduleRowData.getEndTime());
						context.write(keyText, valueText);
					}
				} catch (ParseException e) {
					e.printStackTrace();
				}

			} else if (filePath.contains(viewlogFileName)) {
				viewlogRowData = new ViewlogRowData(line.toString(), ",");
				// 创建viewlog数据对象数组，用于存放切分出的多个viewlog数据对象
				ArrayList<ViewlogRowData> dataArray = null;

				// 将viewlog数据对象进行切分
				try {
					dataArray = ViewlogRowDataSplit.split(viewlogRowData);
					// 只处理观看直播的viewlog数据
					if (viewlogRowData.getLogType().equals("c")) {
						// 遍历数组中所有的viewlog数据对象
						for (int i = 0; i < dataArray.size(); i++) {
							ViewlogRowData tmpData = dataArray.get(i);

							/*
							 * 如果不是计算日报，则计算日期的值从参数“CalcDate”中取得.
							 * 如果是计算日报，则计算日期从当前viewlog中的starttime中取得
							 */
							if (ViewlogOptionUtil.CalcType.D != ViewlogOptionUtil.CalcType.valueOf(dateType)) {
								startDateStr = context.getConfiguration().get("CalcDate");
							} else {
								startDateStr = ViewlogDateUtil.getDate(tmpData.getStartTime());
							}

							// sqoop只支持全格式的日期
							startDateStr += " 00:00:00.0";
							
							//	用key作为关联字段，将两个文件进行关联
							keyText.set(tmpData.getMediaCode());
							valueText.set(viewlogFileFlag + "|" + startDateStr + "|" + tmpData.getUserId() + "|"
									+ tmpData.getAreaCode() + "|" + tmpData.getHdFlag() + "|" + tmpData.getStartTime()
									+ "|" + tmpData.getEndTime());

							context.write(keyText, valueText);
						}
					}
				} catch (ParseException e) {
					e.printStackTrace();
					System.out.println("Parse error, line : " + line.toString());
				}

			}

		}
	}
}
