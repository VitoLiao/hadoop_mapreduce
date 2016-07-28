package com.mr;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.util.ViewlogDateUtil;
import com.util.ViewlogOptionUtil;
import com.util.ViewlogRowData;
import com.util.ViewlogRowDataSplit;
import com.util.ViewlogStringUtil;

public class UserViewAreaMapper1 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// output
		// KEY: VALUE:
		// a|DATE|AreaCode|LogType	TimeInterval   or  b|DATE|AreaCode|UserId	1
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
				if (tmpData.getLogType().equals("c") | tmpData.getLogType().equals("t")
						| tmpData.getLogType().equals("v")) {

					String startDate = null;

//					if (ViewlogOptionUtil.CalcType.D != ViewlogOptionUtil.CalcType.valueOf(dateType)) {
//						startDate = context.getConfiguration().get("CalcDate");
//					} else {
//						startDate = ViewlogDateUtil.getDate(tmpData.getStartTime());
//					}
//
//					Date calcDate = null;
//					String calcDateStr = null;
//
//					try {
//						calcDate = ViewlogDateUtil.str2date(context.getConfiguration().get("CalcDate"), "yyyymmdd");
//						calcDateStr = ViewlogDateUtil.date2str(calcDate, "yyyy-mm-dd");
//					} catch (ParseException e1) {
//						e1.printStackTrace();
//					}

					startDate = context.getConfiguration().get("CalcDate");
					boolean isNeedCalc = false;
					isNeedCalc = ViewlogOptionUtil.isNeedCalc(startDate, ViewlogDateUtil.getDate(tmpData.getStartTime()), dateType);

					if (isNeedCalc) {
						startDate += " 00:00:00.0";
						if (Integer.valueOf(tmpData.getTimeInterval()) > 0) {
							keyText.set("a"+"|"+startDate+"|"+tmpData.getAreaCode()+"|"+tmpData.getLogType());
							valueText.set(String.valueOf(tmpData.getTimeInterval()));
							context.write(keyText, valueText);
//							System.out.println("kv= "+keyText+"|"+valueText);
							
							keyText.set("b"+"|"+startDate+"|"+tmpData.getAreaCode()+"|"+tmpData.getUserId());
							valueText.set("1");
							context.write(keyText, valueText);
						}
					}
				}
			}
		}
	}
}
