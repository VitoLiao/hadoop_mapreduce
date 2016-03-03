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

			// ������ò���
			String startDateStr = null;
			String dateType = context.getConfiguration().get("DateType");
			int dateNum = Integer.parseInt(context.getConfiguration().get("DayNum"));

			// ������ת���GBKģʽ
			Text line = ViewlogStringUtil.transform(value, "GBK");

			// ����schedule��viewlog�����ݶ���
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

							/*
							 * ������Ǽ����ձ�����������ڵ�ֵ�Ӳ�����CalcDate����ȡ��.
							 * ����Ǽ����ձ�����������ڴӵ�ǰviewlog�е�starttime��ȡ��
							 */
							if (ViewlogOptionUtil.CalcType.D != ViewlogOptionUtil.CalcType.valueOf(dateType)) {
								startDateStr = context.getConfiguration().get("CalcDate");
							} else {
								startDateStr = ViewlogDateUtil.getDate(tmpData.getStartTime());
							}

							// sqoopֻ֧��ȫ��ʽ������
							startDateStr += " 00:00:00.0";
							
							//	��key��Ϊ�����ֶΣ��������ļ����й���
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
