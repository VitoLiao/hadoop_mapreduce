package com.mr;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.util.ViewlogDateUtil;
import com.util.ViewlogOptionUtil;
import com.util.ViewlogRowData;
import com.util.ViewlogRowDataSplit;
import com.util.ViewlogStringUtil;

public class ChannelVitalityMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private Text keyText = new Text();
	private static final IntWritable one = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//ֻ�������ݲ�Ϊ�յ�����
		if (value.getLength() > 0) {
			
			//������ת���GBKģʽ
			Text line = ViewlogStringUtil.transform(value, "GBK");
			

			// ���������л��viewlog���ݶ���
			ViewlogRowData viewlogRowData = null;
			
			try {
				viewlogRowData = new ViewlogRowData(line.toString(), ",");
			} catch (Exception e) {
				System.out.println(e.getMessage());
				return;
			}
			
			//����viewlog���ݶ������飬���ڴ���зֳ��Ķ��viewlog���ݶ���
			ArrayList<ViewlogRowData> dataArray = null;
			
			//��viewlog���ݶ�������з�
			try {
				dataArray = ViewlogRowDataSplit.split(viewlogRowData);
			} catch (ParseException e) {
				e.printStackTrace();
				System.out.println("Parse error, line : " + line.toString());
				return;				
			}
			
			//ֻ����ۿ�ֱ����viewlog����
			if (viewlogRowData.getLogType().equals("c")) {
				//�������������е�viewlog���ݶ���
				for (int i = 0; i < dataArray.size(); i++) {
					ViewlogRowData tmpData = dataArray.get(i);
					
					boolean isNeedCalc = false;
					String startDate = null;
					String dateType = context.getConfiguration().get("DateType");

					startDate = context.getConfiguration().get("CalcDate");
					isNeedCalc = ViewlogOptionUtil.isNeedCalc(startDate, ViewlogDateUtil.getDate(tmpData.getStartTime()), dateType);
					
					// ֻ������Ҫ����������ڣ��������ɲ���"-s"����(-s Ĭ��Ϊ����)
					if (isNeedCalc) {				
						// sqoopֻ֧��ȫ��ʽ������
						startDate += " 00:00:00.0";
						
						//keyΪmonthday + userid + channelcode + areacode + hdflag
						keyText.set(startDate + "|" + tmpData.getUserId() + "|" + tmpData.getMediaCode() + "|" + tmpData.getAreaCode() + "|" +  tmpData.getHdFlag() + "|" + dateType);
						
						context.write(keyText, one);
					}
				}			
			}
		}
	}
}
