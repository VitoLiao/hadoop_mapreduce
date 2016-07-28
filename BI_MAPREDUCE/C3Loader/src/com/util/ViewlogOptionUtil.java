package com.util;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;

/**
 * @author VitoLiao
 * @version 2016��2��26�� ����2:10:07
 */
public class ViewlogOptionUtil {

	public enum CalcType {
		D, W, M, Y
	}

	private String calcStartDate = new String();
	private int dayNum = 1;
	CalcType calcType = CalcType.D;
	private int daysDirection = 1;
//	private String hdfsActiveHost = "10.80.248.12";
	private String hdfsActiveHost = "";
	private String hdfsSecondaryHost = null;
	private int port = 0;

	public String getHdfsActiveHost() {
		return hdfsActiveHost;
	}

	public void setHdfsActiveHost(String hdfsActiveHost) {
		this.hdfsActiveHost = hdfsActiveHost;
	}

	public String getHdfsSecondaryHost() {
		return hdfsSecondaryHost;
	}

	public void setHdfsSecondaryHost(String hdfsSecondaryHost) {
		this.hdfsSecondaryHost = hdfsSecondaryHost;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getDaysDirection() {
		return daysDirection;
	}

	public void setDaysDirection(int daysDirection) {
		this.daysDirection = daysDirection;
	}

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

	public ViewlogOptionUtil(String[] args) throws ParseException {
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-s") && i + 1 < args.length && ViewlogDateUtil.isValidDate(args[i + 1])) {
				this.setCalcStartDate(args[i + 1]);
			} else if (args[i].equals("-t") && i + 1 < args.length) {
				if (args[i + 1].equalsIgnoreCase("d")) {
					this.setCalcType(CalcType.D);
				} else if (args[i + 1].equalsIgnoreCase("w")) {
					this.setCalcType(CalcType.W);
				} else if (args[i + 1].equalsIgnoreCase("m")) {
					this.setCalcType(CalcType.M);
				} else if (args[i + 1].equalsIgnoreCase("y")) {
					this.setCalcType(CalcType.Y);
				} else {
					this.setCalcType(CalcType.D);
				}
			} else if (args[i].equals("-n") && i + 1 < args.length && ViewlogStringUtil.isNumeric(args[i + 1])) {
				this.setDayNum(Integer.parseInt(args[i + 1]));
			} else if (args[i].equals("-f") && i + 1 < args.length) {
				this.setDaysDirection(Integer.parseInt(args[i + 1]));
			} else if (args[i].equals("-host") && i + 1 < args.length) {
				String []str = args[i + 1].split(":");
				this.setHdfsActiveHost(str[0]);
				this.setPort(Integer.parseInt(str[1]));
			}
		}

		if (0 == this.getCalcStartDate().length()) {
			Date currentDate = new Date();
			Date yesterdayDate = ViewlogDateUtil.calcWithDayDate(currentDate, -1);
			this.setCalcStartDate(ViewlogDateUtil.date2str(yesterdayDate, "yyyy-MM-dd"));
		}

//		if (CalcType.W == this.getCalcType()) {
//			System.out.println("this.getCalcStartDate() : " + this.getCalcStartDate());
//			Date startDate = ViewlogDateUtil
//					.getWeekMonday(ViewlogDateUtil.str2date(this.getCalcStartDate(), "yyyy-MM-dd"));
//			this.setCalcStartDate(ViewlogDateUtil.date2str(startDate, "yyyy-MM-dd"));
//			System.out.println(ViewlogDateUtil.date2str(startDate, "yyyy-MM-dd"));
//		} else if (CalcType.M == this.getCalcType()) {
//			Date startDate = ViewlogDateUtil
//					.getFirstDayOfMonth(ViewlogDateUtil.str2date(this.getCalcStartDate(), "yyyy-MM-dd"));
//			this.setCalcStartDate(ViewlogDateUtil.date2str(startDate, "yyyy-MM-dd"));			
//		}
	}

	public String getViewlogPath() {
		return "hdfs://" + this.getHdfsActiveHost() + ":" + this.getPort() + "/utsc/input_log/Contentviewlog_";
	}
	
	public String getOnlinelogPath() {
		return "hdfs://" + this.getHdfsActiveHost() + ":" + this.getPort() + "/utsc/input_log/Onlinelog_";
	}	

	public void load(String filePath) throws IOException {
		// Properties pt = new Properties();
		//
		//// InputStream in = Object.class.getResourceAsStream(filePath);
		//
		// pt.load(Thread.currentThread().getClass().getResourceAsStream(filePath));
		//
		// this.setHdfsActiveHost(pt.getProperty("hdfs_active_host").trim());
		// this.setHdfsSecondaryHost(pt.getProperty("hdfs_secondary_host").trim());
		// this.setPort(Integer.parseInt(pt.getProperty("hdfs_port").trim()));
	}

	public ArrayList<String> getViewlogPathList() throws ParseException {
		ArrayList<String> pathList = new ArrayList<String>();

		String startDay = this.getCalcStartDate();

		for (int i = 0; i < this.getDayNum(); i++) {
			Date tmpDate = ViewlogDateUtil.calcWithDayDate(ViewlogDateUtil.str2date(startDay, "yyyy-MM-dd"),
					i * this.getDaysDirection());
			String filePath = this.getViewlogPath() + ViewlogDateUtil.date2str(tmpDate, "yyyyMMdd") + ".log";
			System.out.println("filePath : " + filePath);
			pathList.add(filePath);
		}

		return pathList;
	}

	public ArrayList<String> getOnlinelogPathList() throws ParseException {
		ArrayList<String> pathList = new ArrayList<String>();

		String startDay = this.getCalcStartDate();

		for (int i = 0; i < this.getDayNum(); i++) {
			Date tmpDate = ViewlogDateUtil.calcWithDayDate(ViewlogDateUtil.str2date(startDay, "yyyy-MM-dd"),
					i * this.getDaysDirection());
			String filePath = this.getOnlinelogPath() + ViewlogDateUtil.date2str(tmpDate, "yyyyMMdd") + ".log";
			System.out.println("filePath : " + filePath);
			pathList.add(filePath);
		}

		return pathList;
	}
	
	public static boolean isNeedCalc(String date, String tmpDate, String type) {
		if (CalcType.D.toString().equals(type)) {
			return (date.equals(tmpDate));
		} else {
			return true;
		}
	}

}
