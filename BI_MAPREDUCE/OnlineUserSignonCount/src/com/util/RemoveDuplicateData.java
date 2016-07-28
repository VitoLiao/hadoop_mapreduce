package com.util;

import java.util.HashSet;
import java.util.List;

public class RemoveDuplicateData {
	public static List remove(List list) {
		HashSet h = new HashSet(list);
		list.clear();
		list.addAll(h);
		return list;
	}

	public static int getUniqueSize(List list) {
		HashSet h = new HashSet(list);
		list.clear();
		list.addAll(h);
		int s = list.size();
		return s;
	}
}
