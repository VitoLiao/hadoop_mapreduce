package com.viewlog.util;

import java.util.HashSet;
import java.util.List;

public class RemoveDuplicateData {
	public static int remove(List list) {
		HashSet h = new HashSet(list);
		list.clear();
		list.addAll(h);
		int s = list.size();
		return s;
	}

}
