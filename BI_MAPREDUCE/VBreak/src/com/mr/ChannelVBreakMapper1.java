package com.mr;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

public class ChannelVBreakMapper1 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// ֻ�������ݲ�Ϊ�յ�����
		if (value.getLength() > 0) {

			// ������ת���GBKģʽ
			Text line = ViewlogStringUtil.transform(value, "GBK");

			// ���������л��viewlog���ݶ���
			ViewlogRowData viewlogRowData = null;

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
			} catch (ParseException e) {
				e.printStackTrace();
				System.out.println("Parse error, line : " + line.toString());
				return;
			}

			// ֻ����ۿ�ֱ����viewlog����
			if (viewlogRowData.getLogType().equals("c")) {
				// �������������е�viewlog���ݶ���
				for (int i = 0; i < dataArray.size(); i++) {
					ViewlogRowData tmpData = dataArray.get(i);

					int BreakTime = 0;	//1/2/3/4/.../24, 1=:0:00~0:59
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					try {
						Date date = sdf.parse(tmpData.getEndTime());
						BreakTime = date.getHours()+1;
					} catch (ParseException e) {
						e.printStackTrace();
					}
					
					boolean isNeedCalc = false;
					String startDate = null;
					String dateType = context.getConfiguration().get("DateType");

					startDate = context.getConfiguration().get("CalcDate");
					if (startDate.equals(ViewlogDateUtil.getDate(tmpData.getStartTime()))) {
						isNeedCalc = true;
					}
					
					// ֻ������Ҫ����������ڣ��������ɲ���"-s"����(-s Ĭ��Ϊ����)
					if (isNeedCalc) {		
						// sqoopֻ֧��ȫ��ʽ������
						startDate += " 00:00:00.0";						
						//key�ĸ�ʽ�� ��ʼ����SDAY<yyyyMMdd>|����CONTENTTYPE<c/t/ct>|MEDIACODE<ChannelCode/ScheduleCode/ChannelCode>|AREACODE<0755/020>|HDFLAG<0/1/2/3>
						//value�ĸ�ʽ: breaktime+timeinterval
						keyText.set(startDate + "|" + "c" + "|" 
							+ ViewlogOptionUtil.CalcType.valueOf(dateType) + "|"  
							+ tmpData.getMediaCode() + "|"
							+ tmpData.getAreaCode() + "|" 
							+ tmpData.getHdFlag());
						valueText.set(String.valueOf(BreakTime+"|"+tmpData.getTimeInterval()));
						
						context.write(keyText, valueText);
					}
				}
			}	//t and ct
			else if (viewlogRowData.getLogType().equals("t")) {
				for (int i = 0; i < dataArray.size(); i++) {
					ViewlogRowData tmpData = dataArray.get(i);

					String startDate = null;
					String dateType = context.getConfiguration().get("DateType");

					/*
					 * ������Ǽ����ձ�����������ڵ�ֵ�Ӳ�����CalcDate����ȡ��.
					 * ����Ǽ����ձ�����������ڴӵ�ǰviewlog�е�starttime��ȡ��
					 */
					if (ViewlogOptionUtil.CalcType.D != ViewlogOptionUtil.CalcType.valueOf(dateType)) {
						startDate = context.getConfiguration().get("CalcDate");
					} else {
						startDate = ViewlogDateUtil.getDate(tmpData.getStartTime());
					}
					
					int BreakTime = 00;	//1/2/3/4/.../24, 1=:0:00~0:59
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					try {
						Date date = sdf.parse(tmpData.getEndTime());
						//BreakTime = date.getDate();
						BreakTime = (sdf.parse(tmpData.getEndTime())).getHours()+1;
					} catch (ParseException e) {
						e.printStackTrace();
					}
					
					String calcDateStr = context.getConfiguration().get("CalcDate");
					
					// ֻ������Ҫ����������ڣ��������ɲ���"-s"����(-s Ĭ��Ϊ����)
					if (startDate.equals(calcDateStr)) {			
						startDate += " 00:00:00.0";						
						//t
						keyText.set(startDate + "|" + "t" + "|" 
							+ ViewlogOptionUtil.CalcType.valueOf(dateType) + "|" 
							+ tmpData.getMediaCode() + "|"
							+ tmpData.getAreaCode() + "|" 
							+ tmpData.getHdFlag());
						valueText.set(String.valueOf(BreakTime+"|"+tmpData.getTimeInterval()));
						context.write(keyText, valueText);
						//ct
						
						if (null == tmpData.getParentObjectCode()
								|| 0 == tmpData.getParentObjectCode().length()
								|| false == ViewlogStringUtil.isNumeric(tmpData.getParentObjectCode())) {
							return;
						}						
						
						keyText.set(startDate + "|" + "ct" + "|" 
								+ ViewlogOptionUtil.CalcType.valueOf(dateType) + "|" 
								+ tmpData.getParentObjectCode().trim() + "|"
								+ tmpData.getAreaCode() + "|" 
								+ tmpData.getHdFlag());
						valueText.set(String.valueOf(BreakTime+"|"+tmpData.getTimeInterval()));
						context.write(keyText, valueText);
					}
				}
			}
		}
	}
}
