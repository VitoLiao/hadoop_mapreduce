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

public class PartViewUserMapper1 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// output
		// KEY: VALUE:
		// DATE|AreaCode|HdFlag|LogType|HOUR|UserId 1
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

//			Date calcDate = null;
//			String calcDateStr = null;
			for (int i = 0; i < dataArray.size(); i++) {
				ViewlogRowData tmpData = dataArray.get(i);
				if (tmpData.getLogType().equals("c") | tmpData.getLogType().equals("t")
						| tmpData.getLogType().equals("v") | tmpData.getLogType().equals("p") 
						| tmpData.getLogType().equals("s")) {

					String startDate = null;

//					DayInfomation info = null;
//					try {
//						info = calcDayViewDuration(ViewlogDateUtil.str2date(tmpData.getStartTime()), ViewlogDateUtil.str2date(tmpData.getEndTime()));
//					} catch (ParseException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//						continue;
//					}
					
//					if (ViewlogOptionUtil.CalcType.D != ViewlogOptionUtil.CalcType.valueOf(dateType)) {
//						startDate = context.getConfiguration().get("CalcDate");
//					} else {
//						startDate = ViewlogDateUtil.getDate(tmpData.getStartTime());
//					}
//
//					try {
//						calcDate = ViewlogDateUtil.str2date(context.getConfiguration().get("CalcDate"), "yyyymmdd");
//						calcDateStr = ViewlogDateUtil.date2str(calcDate, "yyyy-mm-dd");
//					} catch (ParseException e1) {
//						e1.printStackTrace();
//					}
					startDate = context.getConfiguration().get("CalcDate");
					boolean isNeedCalc = false;
//					System.out.println("startDate "+startDate);
//					System.out.println(ViewlogDateUtil.getDate(tmpData.getStartTime()));
//					System.out.println("dateType= "+dateType);
					isNeedCalc = ViewlogOptionUtil.isNeedCalc(startDate, ViewlogDateUtil.getDate(tmpData.getStartTime()), dateType);

					if (isNeedCalc) {
						startDate += " 00:00:00.0";
						
						// DATE|AreaCode|HdFlag|LogType|UserId 1
						keyText.set(startDate+"|"+tmpData.getAreaCode()+"|"+tmpData.getHdFlag()+"|"
								+tmpData.getLogType()+"|"+tmpData.getUserId());
						valueText.set("1");
						context.write(keyText, valueText);
//						System.out.println("value= "+value);
						// DATE|AreaCode|HdFlag|LogType|HOUR|UserId 1
						if (Integer.valueOf(tmpData.getTimeInterval()) > 0) {
							String hourStart = tmpData.getStartTime().substring(11,13);
							String hourEnd = tmpData.getEndTime().substring(11,13);
							if (Integer.valueOf(hourStart)<=Integer.valueOf(hourEnd)){
								for(int h=Integer.valueOf(hourStart);h<=Integer.valueOf(hourEnd);h++){
//									System.out.println("in");
									keyText.set(startDate+"|"+tmpData.getAreaCode()+"|"+tmpData.getHdFlag()+"|"
											+tmpData.getLogType()+"|"+h+"|"+tmpData.getUserId());
									valueText.set("1");
									context.write(keyText, valueText);
								}
							}
						}
					}
				}
			}
		}
	}
}
