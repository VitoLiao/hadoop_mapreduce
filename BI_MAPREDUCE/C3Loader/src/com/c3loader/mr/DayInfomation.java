package com.c3loader.mr;

public class DayInfomation {
	public static final int HOUR_COUNT = 24;
	private int[] viewDuration = new int[HOUR_COUNT];
	private int[] viewTimes = new int[HOUR_COUNT];
	private int[] viewUsers = new int[HOUR_COUNT];
	private int viewDurationGt5Min = 0;
	private int viewDurationGt5MinTimes = 0;
	private int viewDurationGt5MinUsers = 0;

	private int viewTotalDuration = 0;

	public String viewDurationToString() {
		String str = ""; 
		for (int i = 0; i < HOUR_COUNT; i++) {
			str += viewDuration[i];
			if (i != HOUR_COUNT - 1) {
				str += "|";
			}
		}
		
		return str;
	}
	public int getViewDurationGt5Min() {
		return viewDurationGt5Min;
	}

	public void setViewDurationGt5Min(int viewDurationGt5Min) {
		this.viewDurationGt5Min = viewDurationGt5Min;
	}

	public int getViewDurationGt5MinTimes() {
		return viewDurationGt5MinTimes;
	}

	public void setViewDurationGt5MinTimes(int viewDurationGt5MinTimes) {
		this.viewDurationGt5MinTimes = viewDurationGt5MinTimes;
	}

	public int getViewDurationGt5MinUsers() {
		return viewDurationGt5MinUsers;
	}

	public void setViewDurationGt5MinUsers(int viewDurationGt5MinUsers) {
		this.viewDurationGt5MinUsers = viewDurationGt5MinUsers;
	}

	public int getViewTotalDuration() {
		return viewTotalDuration;
	}

	public void setViewTotalDuration(int viewTotalDuration) {
		this.viewTotalDuration = viewTotalDuration;
	}

	public int[] getViewDuration() {
		return viewDuration;
	}

	public void setViewDuration(int[] viewDuration) {
		this.viewDuration = viewDuration;
	}

	public int[] getViewTimes() {
		return viewTimes;
	}

	public void setViewTimes(int[] viewTimes) {
		this.viewTimes = viewTimes;
	}

	public int[] getViewUsers() {
		return viewUsers;
	}

	public void setViewUsers(int[] viewUsers) {
		this.viewUsers = viewUsers;
	}
	
	public void print() {
		System.out.println("viewTotalDuration : " + viewTotalDuration);
		for (int i = 0; i < HOUR_COUNT; i++) {
			System.out.println("viewDuration[" + i +"] : " + viewDuration[i]);
		}
	}

}