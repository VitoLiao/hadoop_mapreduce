package com.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class ViewlogDateUtil {

	public static boolean isValidDate(String str) {
		boolean convertSuccess = true;
		// ָ�����ڸ�ʽΪ��λ��/��λ�·�/��λ���ڣ�ע��yyyy-MM-dd HH:mm:ss���ִ�Сд��
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		try {
			// ����lenientΪfalse.
			// ����SimpleDateFormat��ȽϿ��ɵ���֤���ڣ�����2007/02/29�ᱻ���ܣ���ת����2007/03/01
			format.setLenient(false);
			format.parse(str);
		} catch (ParseException e) {
			// e.printStackTrace();
			// ���throw
			// java.text.ParseException����NullPointerException����˵����ʽ����
			convertSuccess = false;
		}
		return convertSuccess;
	}

	/*
	 * public static boolean isValidDate(String dateStr) { String rexp =
	 * "^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))";
	 * Pattern pat = Pattern.compile(rexp); Matcher mat = pat.matcher(dateStr);
	 * boolean dateType = mat.matches(); return dateType; }
	 */

	public static Date str2date(String dateStr, String formatStr) throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat(formatStr);
		return format.parse(dateStr);
	}

	public static Date str2date(String dateStr) throws ParseException {
		return str2date(dateStr, "yyyy-MM-dd HH:mm:ss");
	}

	public static String date2str(Date date, String formatStr) throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat(formatStr);
		return format.format(date);
	}

	public static String date2str(Date date) throws ParseException {
		return date2str(date, "yyyy-MM-dd HH:mm:ss");
	}

	public static int getWeekOfDate(Date date) {
		// String[] weekDaysName = { "������", "����һ", "���ڶ�", "������",
		// "������", "������", "������" };
		// String[] weekDaysCode = { "0", "1", "2", "3", "4", "5", "6" };
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
			// �ҵ������������ܵ���������
			firstDay = calcWithDayDate(date, 0 - weekOfDay);
		}
		return firstDay;
	}

	public static String getDate(String date) {
		return (date.split(" "))[0];
	}

	public static Date calcWithDayDate(Date date, int days) throws ParseException {

		Calendar c = Calendar.getInstance();
		c.setTime(date); // ���õ�ǰ����
		c.add(Calendar.DATE, days); // ���ڼ�1��
		date = c.getTime();

		return date;
	}

	public static String changeDateFormat(String dateStr, String formatBefore, String formateAfter)
			throws ParseException {
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

	public static boolean hasIntersection(Date aStartTime, Date aEndTime, Date bStartTime, Date bEndTime) {
		if (bStartTime.getTime() <= aEndTime.getTime() && bEndTime.getTime() >= aStartTime.getTime()) {
			return true;
		}
		return false;
	}

	public static long getIntersection(Date aStartTime, Date aEndTime, Date bStartTime, Date bEndTime) {

		if (true == ViewlogDateUtil.hasIntersection(aStartTime, aEndTime, bStartTime, bEndTime)) {
			return (Math.min(aEndTime.getTime(), bEndTime.getTime())
					- Math.max(aStartTime.getTime(), bStartTime.getTime())) / 1000;
		}

		return 0;
	}

	public static boolean isInZone(Date targetTime, Date startTime, Date endTime) {
		if (targetTime.getTime() >= startTime.getTime() && targetTime.getTime() <= endTime.getTime()) {
			return true;
		}
		return false;
	}

	public static long intersectionInterval(Date aStartTime, Date aEndTime, Date bStartTime, Date bEndTime) {
		if (hasIntersection(aStartTime, aEndTime, bStartTime, bEndTime)) {
			return ((Math.min(aEndTime.getTime(), bEndTime.getTime())
					- Math.max(aStartTime.getTime(), bStartTime.getTime())) / 1000);
		}
		return 0;
	}

	public static int getHourOfDay(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);

		return c.get(Calendar.HOUR_OF_DAY);
	}

	public static int getMinuteOfDay(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);

		return c.get(Calendar.MINUTE);
	}

	public static int getSecondOfDay(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);

		return c.get(Calendar.SECOND);
	}

	public static Date getLastWeekMonthday(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);

		c.setFirstDayOfWeek(Calendar.MONDAY);
		c.add(Calendar.WEEK_OF_MONTH, -1);
		c.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);

		return c.getTime();
	}

	public static Date getWeekMonday(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);

		c.setFirstDayOfWeek(Calendar.MONDAY);
		c.add(Calendar.WEEK_OF_MONTH, 0);
		c.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);

		return c.getTime();
	}

	public static Date getFirstDayOfMonth(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);

		// c.setFirstDayOfWeek(Calendar.MONDAY);
		c.add(Calendar.MONTH, 0);
		c.set(Calendar.DAY_OF_MONTH, 1);

		return c.getTime();
	}

	public static Date getFirstDayOfYear(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);

		// c.setFirstDayOfWeek(Calendar.MONDAY);
		c.add(Calendar.YEAR, 0);
		c.set(Calendar.DAY_OF_YEAR, 1);

		return c.getTime();
	}
	
	public static Date getLastDayOfMonth(Date sDate1) {
		Calendar c = Calendar.getInstance();
		c.setTime(sDate1);
		final int lastDay = c.getActualMaximum(Calendar.DAY_OF_MONTH);
		c.set(Calendar.DAY_OF_MONTH, lastDay);
		return c.getTime();
	}

	public static Date getLastDayOfYear(Date sDate1) {
		Calendar c = Calendar.getInstance();
		c.setTime(sDate1);
		final int lastDay = c.getActualMaximum(Calendar.DAY_OF_YEAR);
		c.set(Calendar.DAY_OF_YEAR, lastDay);
		return c.getTime();
	}
	
	public static int getMonthdayNumber(Date sDate1) {
		Calendar c = Calendar.getInstance();
		c.setTime(sDate1);
		final int lastDay = c.getActualMaximum(Calendar.DAY_OF_MONTH);
		final int firstDay = c.getActualMinimum(Calendar.DAY_OF_MONTH);
		
		return (lastDay - firstDay + 1);
		
	}
	
	public static int getYeardayNumber(Date sDate1) {
		Calendar c = Calendar.getInstance();
		c.setTime(sDate1);
		final int lastDay = c.getActualMaximum(Calendar.DAY_OF_YEAR);
		final int firstDay = c.getActualMinimum(Calendar.DAY_OF_YEAR);
		
		return (lastDay - firstDay + 1);
		
	}
	
	public static int getDaysBetween(Date smdate, Date bdate) throws ParseException {
		Calendar cal = Calendar.getInstance();
		cal.setTime(smdate);
		long time1 = cal.getTimeInMillis();
		cal.setTime(bdate);
		long time2 = cal.getTimeInMillis();
		long between_days = (time2 - time1) / (1000 * 3600 * 24);

		return Integer.parseInt(String.valueOf(between_days));
	}
}
