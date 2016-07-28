package com.util;

import java.text.ParseException;

/**
 * @author VitoLiao
 * @version 2016��2��29�� ����10:18:09
 */
public class ScheduleRowData {
	private String id = new String();
	private String startTime = new String();
	private String endTime = new String();
	private String scheduleCode = new String();
	private String scheduleName = new String();
	private String channelCode = new String();
	private String channelName = new String();

	public ScheduleRowData(String line, String splitSign) throws ParseException {
		String str[] = line.split(splitSign);

		for (int i = 0; i < str.length; i++) {
			switch (i) {
			case 0:
				id = str[i];
				break;
			case 1:
				startTime = str[i];
				break;
			case 2:
				endTime = str[i];
				break;
			case 3:
				scheduleCode = str[i];
				break;
			case 4:
				scheduleName = str[i];
				break;
			case 5:
				channelCode = str[i];
				break;
			case 6:
				channelName = str[i];
				break;

			default:
				break;
			}
		}

		if (0 == startTime.length() || 0 == endTime.length() || 0 == scheduleCode.length() || 0 == scheduleName.length()
				|| 0 == channelCode.length() || 0 == channelName.length()) {
			throw new ParseException("Schedule invalid : " + line, 0);
		}
	}

	public void print() {
		System.out.println("id : " + this.getId());
		System.out.println("startTime : " + this.getStartTime());
		System.out.println("endTime : " + this.getEndTime());
		System.out.println("scheduleCode : " + this.getScheduleCode());
		System.out.println("scheduleName : " + this.getScheduleName());
		System.out.println("channelCode : " + this.getChannelCode());
		System.out.println("channelName : " + this.getChannelName());
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
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

	public String getScheduleCode() {
		return scheduleCode;
	}

	public void setScheduleCode(String scheduleCode) {
		this.scheduleCode = scheduleCode;
	}

	public String getScheduleName() {
		return scheduleName;
	}

	public void setScheduleName(String scheduleName) {
		this.scheduleName = scheduleName;
	}

	public String getChannelCode() {
		return channelCode;
	}

	public void setChannelCode(String channelCode) {
		this.channelCode = channelCode;
	}

	public String getChannelName() {
		return channelName;
	}

	public void setChannelName(String channelName) {
		this.channelName = channelName;
	}

}
