package com.viewlog.dataobj;

import java.text.ParseException;

public class ViewlogRowData {

	private String id = new String();
	private String userId = new String();
	private String areaCode = new String();
	private String startTime = new String();
	private String endTime = new String();
	private int timeInterval = 0;
	private String logType = new String();
	private String mediaName = new String();
	private String mediaCode = new String();
	private String hdFlag = new String();
	private String parentObjectCode = new String();

	public ViewlogRowData(String line, String splitSign) throws ArrayIndexOutOfBoundsException, ParseException {

		String str[] = line.split(splitSign);
		int i = 0;
		while (i < str.length) {
			String value = str[i];
			switch (i) {
			case 0:
				this.setId(value);
				break;
			case 1:
				this.setLogType(value);
				break;
			case 2:
				this.setAreaCode(value);
				break;
			case 3:
				this.setUserId(value);
				break;
			case 4:
				this.setMediaCode(value);
				break;
			case 5:
				this.setStartTime(value);
				break;
			case 6:
				this.setEndTime(value);
				break;
			case 7:
				this.setTimeInterval(Integer.parseInt(value));
				break;
			case 8:
				break;
			case 9:
				break;
			case 10:
				break;
			case 11:
				break;
			case 12:
				this.setHdFlag(value);
				break;
			case 13:
				break;
			case 14:
				break;
			case 15:
				this.setParentObjectCode(value);
				break;

			}

			i++;
		}

		if (!this.isValid()) {
			throw new ParseException("Viewlog invalid : " + line, 0);
		}

	}

	public boolean isValid() {

		if ((this.getUserId().length() * this.getStartTime().length() * this.getEndTime().length()
				* this.getAreaCode().length() * this.getLogType().length() * this.getMediaCode().length()
				* this.getHdFlag().length()) == 0) {
			return false;
		}

		if (this.getMediaCode().equals("null") || this.getMediaCode().equals("undefined")) {
			return false;
		}

		return true;
	}

	public ViewlogRowData(ViewlogRowData rowData) {
		this.setUserId(rowData.getUserId());
		this.setAreaCode(rowData.getAreaCode());
		this.setStartTime(rowData.getStartTime());
		this.setEndTime(rowData.getEndTime());
		this.setLogType(rowData.getLogType());
		this.setMediaCode(rowData.getMediaCode());
		this.setMediaName(rowData.getMediaName());
		this.setHdFlag(rowData.getHdFlag());
		this.setId(rowData.getId());
		this.setParentObjectCode(rowData.getParentObjectCode());
		this.setTimeInterval(rowData.getTimeInterval());
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
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

	public int getTimeInterval() {
		return timeInterval;
	}

	public void setTimeInterval(int timeInterval) {
		this.timeInterval = timeInterval;
	}

	public String getMediaCode() {
		return mediaCode;
	}

	public void setMediaCode(String mediaCode) {
		this.mediaCode = mediaCode;
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

	public String getHdFlag() {
		return hdFlag;
	}

	public void setHdFlag(String hdFlag) {
		this.hdFlag = hdFlag;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public String getLogType() {
		return logType;
	}

	public void setLogType(String logType) {
		this.logType = logType;
	}

	public String getMediaName() {
		return mediaName;
	}

	public void setMediaName(String mediaName) {
		this.mediaName = mediaName;
	}

	public String getParentObjectCode() {
		return parentObjectCode;
	}

	public void setParentObjectCode(String parentObjectCode) {
		this.parentObjectCode = parentObjectCode;
	}

	public void print() {
		System.out.println("id : " + this.getId());
		System.out.println("userid : " + this.getUserId());
		System.out.println("areacode : " + this.getAreaCode());
		System.out.println("starttime : " + this.getStartTime());
		System.out.println("endtime : " + this.getEndTime());
		System.out.println("logtype : " + this.getLogType());
		System.out.println("hdFlag : " + this.getHdFlag());
		System.out.println("medianame : " + this.getMediaName());
		System.out.println("mediacode : " + this.getMediaCode());
		System.out.println("timeinterval : " + this.getTimeInterval());
		System.out.println("getParentObjectCode : " + this.getParentObjectCode());

	}

}
