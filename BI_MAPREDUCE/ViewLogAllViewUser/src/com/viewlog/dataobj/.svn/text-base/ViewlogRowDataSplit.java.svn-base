package com.viewlog.dataobj;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import com.viewlog.util.ViewlogDateUtil;

public class ViewlogRowDataSplit {
	
    public static int getDaysBetween(Date smdate,Date bdate) throws ParseException{  
        Calendar cal = Calendar.getInstance();    
        cal.setTime(smdate);    
        long time1 = cal.getTimeInMillis();                 
        cal.setTime(bdate);    
        long time2 = cal.getTimeInMillis();         
        long between_days=(time2-time1)/(1000*3600*24);  
            
       return Integer.parseInt(String.valueOf(between_days));     
    }  
    
	public static long getDateTimeoffset(Date startDate, Date endDate) {
		return ((endDate.getTime() - startDate.getTime()) / 1000);
	}
	
	public static boolean isNeedSplit(ViewlogRowData data) {
		String startDate = (data.getStartTime().split(" "))[0];
		String endDate = (data.getEndTime().split(" "))[0];
		
//		long timeOffset = getDateTimeoffset(startTime, endTime);
//		int timeOffset = data.getTimeInterval();
		
		//System.out.println(endDate.getTime() + " - " + startDate.getTime() + " = " + (endDate.getTime() - startDate.getTime()));
		
		if (!startDate.equals(endDate)) { 
			return true;
		}
		
		return false;
	}

	public static ArrayList<ViewlogRowData> split(ViewlogRowData rowData) throws ParseException {
		return split(rowData, "yyyy-MM-dd HH:mm:ss");
	}
	
	public static ArrayList<ViewlogRowData> split(ViewlogRowData rowData, String formatStr) throws ParseException {
		
		ArrayList<ViewlogRowData> list = new ArrayList<ViewlogRowData>();
		
		if (!isNeedSplit(rowData)) {
			list.add(rowData);
			return list;
		}
		
		Date rowStartDate = ViewlogDateUtil.str2date(rowData.getStartTime(), formatStr);
		Date rowEndDate = ViewlogDateUtil.str2date(rowData.getEndTime(), formatStr);
		
		int daysOffset = getDaysBetween(rowStartDate, rowEndDate);
		
//		System.out.println("daysOffset : " + daysOffset);
		
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
			
			ViewlogRowData tmpData = new ViewlogRowData(rowData);
			tmpData.setStartTime(ViewlogDateUtil.date2str(startDateTime, formatStr));
			tmpData.setEndTime(ViewlogDateUtil.date2str(endDateTime, formatStr));
			tmpData.setTimeInterval((int)getDateTimeoffset(startDateTime, endDateTime));
			
			list.add(tmpData);
		}
		
		
		return list;
	}  
}
