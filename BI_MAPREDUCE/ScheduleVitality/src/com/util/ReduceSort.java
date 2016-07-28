package com.util;

import java.util.Comparator;

/**
 * @author VitoLiao
 * @version 2016��4��12�� ����2:09:49
 */
public class ReduceSort implements Comparator {
	public int compare(Object obja, Object objb) {

		ScheduleReduceData a = (ScheduleReduceData)obja;
		ScheduleReduceData b = (ScheduleReduceData)objb;

		return a.getStartTime().compareTo(b.getStartTime());
	}
}
