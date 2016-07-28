package com.viewlog.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ViewlogDateUtil {
	
	public static boolean isValidDate(String str) {
		boolean convertSuccess = true;
		// 指定日期格式为四位年/两位月份/两位日期，注意yyyy-MM-dd HH:mm:ss区分大小写；
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		try {
			// 设置lenient为false.
			// 否则SimpleDateFormat会比较宽松地验证日期，比如2007/02/29会被接受，并转换成2007/03/01
			format.setLenient(false);
			format.parse(str);
		} catch (ParseException e) {
			// e.printStackTrace();
			// 如果throw java.text.ParseException或者NullPointerException，就说明格式不对
			convertSuccess = false;
		}
		return convertSuccess;
	}
	
/*	public static boolean isValidDate(String dateStr) {
		String rexp = "^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))";
		Pattern pat = Pattern.compile(rexp);
		Matcher mat = pat.matcher(dateStr);
		boolean dateType = mat.matches();
		return dateType;
	}*/
	
	public static Date str2date(String dateStr, String formatStr) throws ParseException {
			SimpleDateFormat format = new SimpleDateFormat(formatStr);
			return format.parse(dateStr);
	}
	
	public static String date2str(Date date, String formatStr) throws ParseException {
			SimpleDateFormat format = new SimpleDateFormat(formatStr);
			return format.format(date);
	}
	
	public static int getWeekOfDate(Date date) {
//		  String[] weekDaysName = { "星期日", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六" };
//		  String[] weekDaysCode = { "0", "1", "2", "3", "4", "5", "6" };
		  Calendar calendar = Calendar.getInstance();
		  calendar.setTime(date);
		  int intWeek = calendar.get(Calendar.DAY_OF_WEEK) - 1;
		  return intWeek;
		} 
	
	public static Date getTheFirstDay(Date date, String dateType) throws ParseException {
		Date firstDay = new Date();
		if (dateType.equalsIgnoreCase("d")) {
			firstDay = date;
		} else if (dateType.equalsIgnoreCase("w")) {
			int weekOfDay = getWeekOfDate(date);
			//找到该日期所在周的周日日期
			firstDay = calcWithDayDate(date, 0 - weekOfDay);
		}
		return firstDay;
	} 
	
	public static String getDate(String date) {
		return (date.split(" "))[0];
	}
	
	public static Date calcWithDayDate(Date date, int days) throws ParseException {
		
		Calendar c = Calendar.getInstance();
		c.setTime(date); // 设置当前日期
		c.add(Calendar.DATE, days); // 日期加1天
		date = c.getTime();
		
		return date;
	}	
	
	public static String changeDateFormat(String dateStr, String formatBefore, String formateAfter) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(formatBefore);
		Date date = sdf.parse(dateStr);
		
		sdf = new SimpleDateFormat(formateAfter);
		return sdf.format(date);
		
	}
	
	public static Date getDayStartTime(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		return c.getTime();
	}
	
	public static Date getDayEndTime(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.set(Calendar.HOUR_OF_DAY, 23);
		c.set(Calendar.MINUTE, 59);
		c.set(Calendar.SECOND, 59);
		return c.getTime();
	}	
}
