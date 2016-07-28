package com.mr;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author VitoLiao
 * @version 2016��4��13�� ����4:56:03
 */
public class ChangeStrAndDate {

	public static Date StrToDate(String Str ,String DateType) {
		SimpleDateFormat sp=new SimpleDateFormat(DateType);
		Date date=null;
		try {
			date=sp.parse(Str);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}
	
	
	public static String MinusDate(String getdate) {
		SimpleDateFormat sp=new SimpleDateFormat("yyyyMMdd");
		Date date=null;
		try {
			date=sp.parse(getdate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		Calendar calendar=Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(5, -1);
		String date1=sp.format(calendar.getTime());
		
		return date1;
	}
	
	public static void main(String[] args) {
		System.out.println(MinusDate("20160112"));
	}
}
 