package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.util.ViewlogDateUtil;
import com.util.ViewlogOptionUtil;
import com.util.ViewlogRowData;
import com.util.ViewlogStringUtil;

/**
 * @author VitoLiao
 * @version 2016��4��13�� ����2:05:04
 */
public class InOutMapper1 extends Mapper<LongWritable, Text, Text, Text> {
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

			// // ����viewlog���ݶ������飬���ڴ���зֳ��Ķ��viewlog���ݶ���
			// ArrayList<ViewlogRowData> dataArray = null;
			//
			// // ��viewlog���ݶ�������з�
			// try {
			// dataArray = ViewlogRowDataSplit.split(viewlogRowData);
			// } catch (ParseException e) {
			// e.printStackTrace();
			// System.out.println("Parse error, line : " + line.toString());
			// return;
			// }

			// ֻ����ۿ�ֱ����viewlog����
			if (viewlogRowData.getLogType().equals("c") || viewlogRowData.getLogType().equals("t")) {
				boolean isNeedCalc = false;
				String startDate = context.getConfiguration().get("CalcDate");;
				String dateType = context.getConfiguration().get("DateType");

				isNeedCalc = ViewlogOptionUtil.isNeedCalc(startDate, ViewlogDateUtil.getDate(viewlogRowData.getStartTime()), dateType);
				
				if (isNeedCalc) {
					startDate += " 00:00:00.0";

					if (viewlogRowData.getLogType().equals("c")) {
						keyText.set(startDate + "|" + "c" + "|" + dateType + "|" + viewlogRowData.getMediaCode().trim() + "|"
								+ viewlogRowData.getAreaCode() + "|" + viewlogRowData.getHdFlag());
					} else if (viewlogRowData.getLogType().equals("t") 
							&& null != viewlogRowData.getParentObjectCode()
							&& 0 != viewlogRowData.getParentObjectCode().length()
							&& true == ViewlogStringUtil.isNumeric(viewlogRowData.getParentObjectCode()))
						
						keyText.set(startDate + "|" + "ct" + "|" + dateType + "|" + viewlogRowData.getParentObjectCode().trim() + "|"
								+ viewlogRowData.getAreaCode() + "|" + viewlogRowData.getHdFlag());
					} else {
						return;
					}

					valueText.set(viewlogRowData.getUserId() + "|" + viewlogRowData.getStartTime() + "|"
							+ viewlogRowData.getEndTime());

					context.write(keyText, valueText);
				}
			}
		}

}
