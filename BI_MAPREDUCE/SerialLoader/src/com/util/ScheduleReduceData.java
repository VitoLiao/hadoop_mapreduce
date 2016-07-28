package com.util;

/**
* @author VitoLiao
* @version 2016��4��12�� ����2:05:23
*/
public class ScheduleReduceData {

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
