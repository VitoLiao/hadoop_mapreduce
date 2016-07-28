package com.mr;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.util.ViewlogRowData;
import com.util.ViewlogRowDataSplit;
import com.util.ViewlogStringUtil;

public class LostUserMapper1 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();
	private IntWritable valueInt = new IntWritable();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// ֻ�������ݲ�Ϊ�յ�����
		if (value.getLength() > 0) {
			// String [] filePaths = ((FileSplit)
			// context.getInputSplit()).getPath().toString().split("/");
			String filePath = context.getConfiguration().get("CalcDate");

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
			if (viewlogRowData.getLogType().equals("c") || viewlogRowData.getLogType().equals("t")) {
				// �������������е�viewlog���ݶ���
				for (int i = 0; i < dataArray.size(); i++) {
					ViewlogRowData tmpData = dataArray.get(i);
					String stime = tmpData.getStartTime().substring(8, 10);
					// �ж����ݶ�Ӧ��ʱ��
					// CalcDate�Ǵ����������������,���ڲ�����Ӧ������

					if (filePath.substring(6, 8).equals(stime)) {

						if (tmpData.getLogType().equals("c")) {
							keyText.set(tmpData.getStartTime().substring(0, 10) + "|" + tmpData.getLogType() + "|"
									+ tmpData.getMediaCode() + "|" + tmpData.getAreaCode() + "|" + tmpData.getHdFlag()
									+ "|" + tmpData.getUserId());
							valueText.set(String.valueOf(tmpData.getTimeInterval()));
						} else if (tmpData.getLogType().equals("t")) {

							keyText.set(tmpData.getStartTime().substring(0, 10) + "|" + "ct" + "|"
									+ tmpData.getParentObjectCode().trim() + "|" + tmpData.getAreaCode() + "|"
									+ tmpData.getHdFlag() + "|" + tmpData.getUserId());
							valueText.set(String.valueOf(tmpData.getTimeInterval()));
						}
						context.write(keyText, valueText);

					}
					// �������ڶ�Ӧ��ǰһ��
					else {
						try {
							// Date date1 =
							// sp.parse(viewlogRowData.getStartTime());
							// Date date2 =
							// sp.parse(viewlogRowData.getEndTime());
							// long usetime =
							// Long.parseLong(tmpData.getTimeInterval());
							// c+ct

							if (tmpData.getLogType().equals("c")) {
								keyText.set(tmpData.getStartTime().substring(0, 10) + "|" + tmpData.getLogType() + "|"
										+ tmpData.getMediaCode() + "|" + tmpData.getAreaCode() + "|"
										+ tmpData.getHdFlag() + "|" + tmpData.getUserId());
								valueText.set(String.valueOf(tmpData.getTimeInterval()));
							} else if (tmpData.getLogType().equals("t")) {

								if (null == tmpData.getParentObjectCode() || 0 == tmpData.getParentObjectCode().length()
										|| false == ViewlogStringUtil.isNumeric(tmpData.getParentObjectCode())) {
									return;
								}

								keyText.set(tmpData.getStartTime().substring(0, 10) + "|" + "ct" + "|"
										+ tmpData.getParentObjectCode().trim() + "|" + tmpData.getAreaCode() + "|"
										+ tmpData.getHdFlag() + "|" + tmpData.getUserId());
								valueText.set(String.valueOf(tmpData.getTimeInterval()));
							}
							context.write(keyText, valueText);

							// System.out.println(usetime);
						} catch (Exception e) {
							// TODO: handle exception
						}

					}

				}
			}

		}
	}

}
