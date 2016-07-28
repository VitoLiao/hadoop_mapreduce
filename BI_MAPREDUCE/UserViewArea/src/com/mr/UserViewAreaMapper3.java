package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserViewAreaMapper3 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
		//input
		//key value
		// DATE|AreaCode|userall
		//DATE|AREACODE|每日收视次数|拆分话单后每日收视次数|每日收视总时长(秒)
		//|每日收视用户数(去重)|点播每日收视次数|频道每日收视次数|回看每日收视次数
		//|点播每日收视时长(秒)|频道每日收视时长(秒)|回看每日收视时长(秒)
		String[] str = value.toString().trim().split("\\|",-1);
//		System.out.println("M3 value = "+value);
		if(str.length<4){
			keyText.set(str[0]+"|"+str[1]);
			valueText.set(str[2]);
			context.write(keyText, valueText);
		}else{
			keyText.set(str[0]+"|"+str[1]);
			valueText.set(value);
			context.write(keyText, valueText);
		}
	}

}
