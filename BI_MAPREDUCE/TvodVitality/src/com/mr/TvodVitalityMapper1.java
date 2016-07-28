package com.mr;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.util.ViewlogDateUtil;
import com.util.ViewlogRowData;
import com.util.ViewlogRowDataSplit;
import com.util.ViewlogStringUtil;

public class TvodVitalityMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
	
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
			if (viewlogRowData.getLogType().equals("t")) {
				//�������������е�viewlog���ݶ���
				for (int i = 0; i < dataArray.size(); i++) {
					ViewlogRowData tmpData = dataArray.get(i);
					
					boolean isNeedCalc = false;
					String startDate = null;

					startDate = context.getConfiguration().get("CalcDate");
					if (startDate.equals(ViewlogDateUtil.getDate(tmpData.getStartTime()))) {
						isNeedCalc = true;
					}
					
					if (null == tmpData.getParentObjectCode()
							|| 0 == tmpData.getParentObjectCode().length()
							|| false == ViewlogStringUtil.isNumeric(tmpData.getParentObjectCode())) {
						isNeedCalc = false;
					}
					
					// ֻ������Ҫ����������ڣ��������ɲ���"-s"����(-s Ĭ��Ϊ����)
					if (isNeedCalc) {		
						// sqoopֻ֧��ȫ��ʽ������
						startDate += " 00:00:00.0";
						
						//keyΪmonthday + userid + channelcode + areacode + hdflag
						keyText.set(startDate + "|" + tmpData.getUserId() + "|" + tmpData.getParentObjectCode().trim() + "|" + tmpData.getMediaCode() + "|" + tmpData.getAreaCode() + "|" +  tmpData.getHdFlag());
						
						context.write(keyText, one);
					}
				}			
			}
		}
	}
}
