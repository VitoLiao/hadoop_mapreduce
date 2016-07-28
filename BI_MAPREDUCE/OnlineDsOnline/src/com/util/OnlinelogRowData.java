package com.util;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

/**
 * @author VitoLiao
 * @version 2016年5月11日 下午2:44:36
 */
public class OnlinelogRowData {
	private String id = null;
	private String userId = null;
	private String areaCode = null;
	private String startTime = null;
	private String endTime = null;
	private String hdFlag = null;

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

	public String getHdFlag() {
		return hdFlag;
	}

	public void setHdFlag(String hdFlag) {
		this.hdFlag = hdFlag;
	}

	public OnlinelogRowData(String line, String splitSign) throws ParseException {
		String str[] = line.split(splitSign);
		int i = 0;
		while (i < str.length) {
			String value = str[i].trim();
			switch (i) {
			case 0:
				this.setId(value);
				break;
			case 1:
				this.setUserId(value);
				break;
			case 2:
				this.setAreaCode(value);
				break;
			case 3:
				this.setStartTime(value);
				break;
			case 4:
				this.setEndTime(value);
				break;
			case 5:
				this.setHdFlag(value);
				break;
			default:
				break;
			}

			i++;
		}
		
		if (false == this.isValid()) {
			throw new ParseException("Viewlog invalid : " + line, 0);
		}
	}

	public boolean isValid() {
		if (0 == this.getUserId().length() 
				|| 0 == this.getAreaCode().length() 
				|| 0 == this.getStartTime().length() 
				|| 0 == this.getEndTime().length()) {
			return false;
		}
		
		return true;
	}
	
	public OnlinelogRowData(OnlinelogRowData rowData) {
		this.setId(rowData.getId());
		this.setUserId(rowData.getUserId());
		this.setStartTime(rowData.getStartTime());
		this.setEndTime(rowData.getEndTime());
		this.setAreaCode(rowData.getAreaCode());
		this.setHdFlag(rowData.getHdFlag());
	}

	public static ArrayList<OnlinelogRowData> split(OnlinelogRowData rowData) throws ParseException {
		ArrayList<OnlinelogRowData> list = new ArrayList<OnlinelogRowData>();
		
		Date rowStartDate = ViewlogDateUtil.str2date(rowData.getStartTime());
		Date rowEndDate = ViewlogDateUtil.str2date(rowData.getEndTime());
		
		int daysOffset = ViewlogDateUtil.getDaysBetween(rowStartDate, rowEndDate);
		
		for (int i = 0; i <= daysOffset; i++) {
			Date startDateTime = new Date();
			Date endDateTime = new Date();
			
			if (0 == i) {
				startDateTime = rowStartDate;
				endDateTime = ViewlogDateUtil.getDayEndTime(rowStartDate);
			} else if (daysOffset == i) {
				endDateTime = rowEndDate;
				startDateTime = ViewlogDateUtil.getDayStartTime(rowEndDate);
			} else {
				Date currentDate = ViewlogDateUtil.calcWithDayDate(rowStartDate, i);
				startDateTime = ViewlogDateUtil.getDayStartTime(currentDate);
				endDateTime = ViewlogDateUtil.getDayEndTime(currentDate);
			}
			
			OnlinelogRowData tmpData = new OnlinelogRowData(rowData);
			tmpData.setStartTime(ViewlogDateUtil.date2str(startDateTime));
			tmpData.setEndTime(ViewlogDateUtil.date2str(endDateTime));
			
			list.add(tmpData);
		}		
		return list;
	}
}
