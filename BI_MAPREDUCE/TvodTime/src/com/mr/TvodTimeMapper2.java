package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
* @author VitoLiao
* @version 2016��3��3�� ����3:31:06
*/
public class TvodTimeMapper2 extends Mapper<LongWritable, Text, Text, IntWritable>{
	private static final IntWritable one = new IntWritable(1);
	private Text keyText = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		if (value.getLength() > 0) {
			String []str = value.toString().trim().split(",");
			String []keyStr = str[0].split("\\|");
			
			String tmpKey = "";
			
			for(int i = 0; i < keyStr.length; i++) {
				if (i != 1) {
					tmpKey += keyStr[i];
					if (i != keyStr.length - 1) {
						tmpKey += "|";
					}
				}
			}
			
			keyText.set(tmpKey + "|" + str[1]);
			
			context.write(keyText, one);			
		}
	}
}
