package com.viewlog.util;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
* @author VitoLiao
* @version 2016年3月3日 上午11:18:41
*/
public class HdfsOptionUtil {
	public static boolean deletePath(String path) throws IOException {
		boolean isDeleted = false;
		
		Configuration conf = new Configuration();
		FileSystem deletFs = FileSystem.get(URI.create(path), conf);
		Path delef = new Path(path);
		
		if (deletFs.delete(delef, true)) {
			isDeleted = true;
		}
		
		return isDeleted;
	}
	
	// 判断路径在hdfs上是否存在
	public static boolean isPathExist(String filePath) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(filePath), conf);
		Path path = new Path(filePath);
		if (fs.exists(path)) {
			return true;
		}
		
		return false;
	}
}
