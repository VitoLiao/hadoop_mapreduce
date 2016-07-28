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
public class ScheduleVitalityMapper1 extends Mapper<LongWritable, Text, Text, Text> {
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
			String dateType = context.getConfiguration().get("DateType");

			// ������ת���GBKģʽ
			Text line = ViewlogStringUtil.transform(value, "GBK");

			// ����schedule��viewlog�����ݶ���
			ScheduleRowData scheduleRowData = null;
			ViewlogRowData viewlogRowData = null;

			if (filePath.contains(scheduleFileName)) {
				try {
					scheduleRowData = new ScheduleRowData(line.toString(), "\\|");
				} catch (ParseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				// ��ý�Ŀ��ʼʱ��ͽ���ʱ���Date����
				Date scheduleStartDate;
				try {
					scheduleStartDate = ViewlogDateUtil.str2date(context.getConfiguration().get("ScheduleStartDate"),
							"yyyyMMdd");
					Date scheduleEndDate = ViewlogDateUtil.str2date(context.getConfiguration().get("ScheduleEndDate"),
							"yyyyMMdd");

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
				try {
					viewlogRowData = new ViewlogRowData(line.toString(), ",");
				} catch (Exception e) {
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

							boolean isNeedCalc = false;
							String startDate = null;

							startDate = context.getConfiguration().get("CalcDate");
							
							if (startDate.equals(ViewlogDateUtil.getDate(tmpData.getStartTime()))) {
								isNeedCalc = true;
							}
							
							// ֻ������Ҫ����������ڣ��������ɲ���"-s"����(-s Ĭ��Ϊ����)
							if (isNeedCalc) {		
								// sqoopֻ֧��ȫ��ʽ������
								startDate += " 00:00:00.0";
								keyText.set(tmpData.getMediaCode());
								valueText.set(viewlogFileFlag + "|" + startDate + "|" + tmpData.getUserId() + "|"
										+ tmpData.getAreaCode() + "|" + tmpData.getHdFlag() + "|" + tmpData.getStartTime()
										+ "|" + tmpData.getEndTime());
	
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
