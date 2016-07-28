package com.mr;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.util.ScheduleRowData;
import com.util.ViewlogDateUtil;
import com.util.ViewlogRowData;
import com.util.ViewlogRowDataSplit;
import com.util.ViewlogStringUtil;

/**
 * @author VitoLiao
 * @version 2016��2��29�� ����9:00:32
 */
public class ScheduleTimeMapper1 extends Mapper<LongWritable, Text, Text, Text> {
	private Text keyText = new Text();
	private Text valueText = new Text();
	private final static String scheduleFileName = "schedule";
	private final static String viewlogFileName = "viewlog";
	private final static String scheduleFileFlag = "s";
	private final static String viewlogFileFlag = "v";

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// ֻ�������ݲ�Ϊ�յ�����
		if (value.getLength() > 0) {

			String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();

			// ������ת���GBKģʽ
			Text line = ViewlogStringUtil.transform(value, "GBK");

			// ����schedule��viewlog�����ݶ���
			ScheduleRowData scheduleRowData = null;
			ViewlogRowData viewlogRowData = null;

			if (filePath.contains(scheduleFileName)) {
				// ���scheduleԭʼ���ݶ���
				try {
					scheduleRowData = new ScheduleRowData(line.toString(), "\\|");
				} catch (ParseException e) {
					e.printStackTrace();
				}

				// ��ý�Ŀ��ʼʱ��ͽ���ʱ���Date����
				try {
					Date scheduleStartDate = ViewlogDateUtil
							.str2date(context.getConfiguration().get("ScheduleStartDate"), "yyyyMMdd");
					Date scheduleEndDate = ViewlogDateUtil.str2date(context.getConfiguration().get("ScheduleEndDate"),
							"yyyyMMdd");

					// ��ȡָ����Χ�ڵ�schedule���ݣ������ص�reduce�С�
					if (ViewlogDateUtil.isInZone(ViewlogDateUtil.str2date(scheduleRowData.getStartTime()),
							scheduleStartDate, scheduleEndDate)) {

						keyText.set(scheduleRowData.getChannelCode());
						valueText.set(scheduleFileFlag + "|" + scheduleRowData.getScheduleCode() + "|"
								+ scheduleRowData.getStartTime() + "|" + scheduleRowData.getEndTime());
						context.write(keyText, valueText);
					}
				} catch (ParseException e1) {
					e1.printStackTrace();
				}

			} else if (filePath.contains(viewlogFileName)) {
				// ���viewlogԭʼ����
				try {
					viewlogRowData = new ViewlogRowData(line.toString(), ",");
				} catch (ParseException e) {
					System.out.println(e.getMessage());
					return;
				}

				// ����viewlog���ݶ������飬���ڴ���зֳ��Ķ��viewlog���ݶ���
				ArrayList<ViewlogRowData> dataArray = null;

				// ��viewlog���ݶ�������з�
				try {
					dataArray = ViewlogRowDataSplit.split(viewlogRowData);
					// ֻ����ۿ�ֱ����viewlog����
					if (viewlogRowData.getLogType().equals("c")) {
						// �������������е�viewlog���ݶ���
						for (int i = 0; i < dataArray.size(); i++) {
							ViewlogRowData tmpData = dataArray.get(i);

							String startDate = context.getConfiguration().get("CalcDate");

							if (startDate.equals(ViewlogDateUtil.getDate(tmpData.getStartTime()))) {
								startDate += " 00:00:00.0";

								keyText.set(tmpData.getMediaCode());
								valueText.set(viewlogFileFlag + "|" + startDate + "|" + tmpData.getUserId() + "|"
										+ tmpData.getAreaCode() + "|" + tmpData.getHdFlag() + "|"
										+ tmpData.getStartTime() + "|" + tmpData.getEndTime());

								context.write(keyText, valueText);
							}
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
