package com.viewlog.util;

import java.text.ParseException;
import java.util.Date;

/**
* @author VitoLiao
* @version 2016年2月26日 下午2:10:07
*/
public class ViewlogOptionUtil {
	
	public enum CalcType {
		D, W, M, Y
	}
	
	private String calcStartDate = new String();
	private int dayNum = 1;
	CalcType calcType;
	
	public String getCalcStartDate() {
		return calcStartDate;
	}

	public void setCalcStartDate(String calcStartDate) {
		this.calcStartDate = calcStartDate;
	}

	public int getDayNum() {
		return dayNum;
	}

	public void setDayNum(int dayNum) {
		this.dayNum = dayNum;
	}

	public CalcType getCalcType() {
		return calcType;
	}

	public void setCalcType(CalcType calcType) {
		this.calcType = calcType;
	}

	public ViewlogOptionUtil(String []args) throws ParseException {
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-s") && i + 1 < args.length && ViewlogDateUtil.isValidDate(args[i + 1])) {
				calcStartDate = args[i + 1];
			} else if (args[i].equals("-t") && i + 1 < args.length) {
				if (args[i + 1].equalsIgnoreCase("d")) {
					this.setCalcType(CalcType.D);
				}else if (args[i + 1].equalsIgnoreCase("w")) {
					this.setCalcType(CalcType.W);
				}else if (args[i + 1].equalsIgnoreCase("m")) {
					this.setCalcType(CalcType.M);
				}else if (args[i + 1].equalsIgnoreCase("y")) {
					this.setCalcType(CalcType.Y);
				}else {
					this.setCalcType(CalcType.D);
				}
			} else if (args[i].equals("-n") && i + 1 < args.length && ViewlogStringUtil.isNumeric(args[i + 1])) {
				this.setDayNum(Integer.parseInt(args[i + 1]));
			}
		}
		
		if (0 == this.getCalcStartDate().length()) {
			Date currentDate = new Date();
			Date yesterdayDate = ViewlogDateUtil.calcWithDayDate(currentDate, -1);
			this.setCalcStartDate(ViewlogDateUtil.date2str(yesterdayDate, "yyyy-MM-dd"));
		}
	}
			
}
