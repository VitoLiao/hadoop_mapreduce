package com.util;

/**
* @author VitoLiao
* @version 2016��4��12�� ����2:07:17
*/
public class ViewlogReduceData {

	private String startDateStr = new String();
	private String userId = new String();
	private String areaCode = new String();
	private String hdflag = new String();
	private String startTime = new String();
	private String endTime = new String();

	public ViewlogReduceData(String rowData) {
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

	public ViewlogReduceData(ViewlogReduceData data) {
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
