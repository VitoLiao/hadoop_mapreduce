package com.schedule.mr.time;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sun.accessibility.internal.resources.accessibility;
import com.viewlog.util.ViewlogDateUtil;

/**
 * @author VitoLiao
 * @version 2016��2��29�� ����10:02:59
 */

class ScheduleReduceData {
	private String ScheduleCode = new String();
	private String startTime = new String();
	private String endTime = new String();

	public ScheduleReduceData(String rowData) {
		String array[] = rowData.split("\\|");
		for (int i = 0; i < array.length; i++) {
			switch (i) {
			case 0:
				this.setScheduleCode(array[i]);
				break;
			case 1:
				this.setStartTime(array[i]);
				break;
			case 2:
				this.setEndTime(array[i]);
				break;
			default:
				break;
			}
		}
	}

	public ScheduleReduceData(ScheduleReduceData data) {
		this.setScheduleCode(data.getScheduleCode());
		this.setStartTime(data.getStartTime());
		this.setEndTime(data.getEndTime());
	}

	public String getScheduleCode() {
		return ScheduleCode;
	}

	public void setScheduleCode(String scheduleCode) {
		ScheduleCode = scheduleCode;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
}

class viewlogReduceData {
	private String startDateStr = new String();
	private String userId = new String();
	private String areaCode = new String();
	private String hdflag = new String();
	private String startTime = new String();
	private String endTime = new String();

	public viewlogReduceData(String rowData) {
		String array[] = rowData.split("\\|");
		for (int i = 0; i < array.length; i++) {
			switch (i) {
			case 0:
				this.setStartDateStr(array[i]);
				break;
			case 1:
				this.setUserId(array[i]);
				break;
			case 2:
				this.setAreaCode(array[i]);
				break;
			case 3:
				this.setHdflag(array[i]);
				break;
			case 4:
				this.setStartTime(array[i]);
				break;
			case 5:
				this.setEndTime(array[i]);
				break;
			default:
				break;
			}
		}
	}

	public viewlogReduceData(viewlogReduceData data) {
		this.setStartDateStr(data.getStartDateStr());
		this.setAreaCode(data.getAreaCode());
		this.setHdflag(data.getHdflag());
		this.setUserId(data.getUserId());
		this.setStartTime(data.getStartTime());
		this.setEndTime(data.getEndTime());
	}

	public String getStartDateStr() {
		return startDateStr;
	}

	public void setStartDateStr(String startDateStr) {
		this.startDateStr = startDateStr;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getAreaCode() {
		return areaCode;
	}

	public void setAreaCode(String areaCode) {
		this.areaCode = areaCode;
	}

	public String getHdflag() {
		return hdflag;
	}

	public void setHdflag(String hdflag) {
		this.hdflag = hdflag;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

}

public class ScheduleTimeReducer1 extends Reducer<Text, Text, Text, IntWritable> {
	private Text keyText = new Text();
	private IntWritable valueText = new IntWritable();
	private final static String scheduleFileFlag = "s";
	private final static String viewlogFileFlag = "v";

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		ArrayList<ScheduleReduceData> scheduleLish = new ArrayList<ScheduleReduceData>();
		ArrayList<viewlogReduceData> viewlogLish = new ArrayList<viewlogReduceData>();
		Map<String, Integer> map = new HashMap<String, Integer>();
		
		for (Text value : values) {
			String valueStr = value.toString();
			String[] str = value.toString().split("\\|");

			String fileFlag = str[0];

			if (fileFlag.equals(scheduleFileFlag)) {
				scheduleLish.add(new ScheduleReduceData(valueStr.substring(str[0].length() + 1, valueStr.length())));
			} else if (fileFlag.equals(viewlogFileFlag)) {
				viewlogLish.add(new viewlogReduceData(valueStr.substring(str[0].length() + 1, valueStr.length())));
			}

		}

		System.out.println("scheduleLish.size() : " + scheduleLish.size());
		System.out.println("viewlogLish.size() : " + viewlogLish.size());

		for (int i = 0; i < scheduleLish.size(); i++) {
			ScheduleReduceData scheduleData = scheduleLish.get(i);
			long viewInterval = 0;
			for (int j = 0; j < viewlogLish.size(); j++) {
				viewlogReduceData viewlogData = viewlogLish.get(j);
				try {
					// �����û��տ���Ŀʱ��
					viewInterval = ViewlogDateUtil.intersectionInterval(
							ViewlogDateUtil.str2date(scheduleData.getStartTime()),
							ViewlogDateUtil.str2date(scheduleData.getEndTime()),
							ViewlogDateUtil.str2date(viewlogData.getStartTime()),
							ViewlogDateUtil.str2date(viewlogData.getEndTime()));

					keyText.set(viewlogData.getStartDateStr() + "|" + viewlogData.getUserId() + "|" + key.toString()
							+ "|" + scheduleData.getScheduleCode() + "|" + viewlogData.getAreaCode() + "|"
							+ viewlogData.getHdflag());
					
					if (viewInterval > 0) {
						if (map.containsKey(keyText.toString())){
							map.put(keyText.toString(), new Integer((int)viewInterval + (int) map.get(keyText.toString())));
						} else {
							map.put(keyText.toString(), new Integer((int)viewInterval));
						}
					}

				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			
			System.out.println("map size : " + map.size());
			for (String itr : map.keySet()) {
				int timeSum = (int)map.get(itr);
				
				//ʱ���־λ�� [1] : x<10min, [2] : 10min <= x < 1h, [3] : 1h <= x < 2h, [4] : 2h <= x < 3h, [5] : 3h <= x < 4h, [6] : 4h <= x 
				int timeFlag = 0;
				if (timeSum < 10 * 60) {
					timeFlag = 1;
				}
				else if (timeSum >= 10 * 60 && timeSum < 60 * 60) {
					timeFlag = 2;
				}
				else if (timeSum >= 60 * 60 && timeSum < 120 * 60) {
					timeFlag = 3;
				}
				else if (timeSum >= 120 * 60 && timeSum < 180 * 60) {
					timeFlag = 4;
				}
				else if (timeSum >= 180 * 60 && timeSum < 240 * 60) {
					timeFlag = 5;
				}
				else if (timeSum >= 240 * 60) {
					timeFlag = 6;
				}
				
				valueText.set(timeFlag);
				keyText.set(itr);
				context.write(keyText, valueText);
			}
			
			map.clear();
		}
	}
}
