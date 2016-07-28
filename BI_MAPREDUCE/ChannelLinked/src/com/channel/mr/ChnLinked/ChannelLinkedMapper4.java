package com.channel.mr.ChnLinked;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChannelLinkedMapper4 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//input
		// CODE###DATE|CONTENTTYPE|TYPE|CODE|USERID|TIMEINTERVAL|STARTTIME|ENDTIME##..###
		// MEDIACODE|STARTTIME|STOPTIME##...
		
		// ot|DATE|CONTENTTYPE|TYPE|CODE|USERID|TIMEINTERVAL
		if (value.getLength() > 0) {
			String[] strings = value.toString().split("###",-1);
//			String C2CODE = strings[0].trim();
			String[] strVL = strings[1].split("##",-1);
			String[] strSD = strings[2].split("##",-1);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			for(int i=0;i<strSD.length;i++){
				String[] sSchedule = strSD[i].trim().split("\\|",-1);
				
				for(int j=0;j<strVL.length;j++){
					String[] sViewLog = strVL[j].trim().split("\\|",-1);
//					String startDate = sViewLog[0];
					try {
						Date dViewLogStart = sdf.parse(sViewLog[6].trim());
						Date dViewLogStop = sdf.parse(sViewLog[7].trim());
						Date dScheduleStart = sdf.parse(sSchedule[1].trim());
						Date dScheduleStop = sdf.parse(sSchedule[2].trim());
						
						if(dViewLogStart.before(dScheduleStart)	& dViewLogStop.after(dScheduleStart)
								& dViewLogStop.before(dScheduleStop)){
							keyText.set(sViewLog[1]+"|"+sViewLog[0]+"|"+sViewLog[1]+"|"+sViewLog[2]+"|"+sViewLog[3]+"|"+sViewLog[4]+"|"+sViewLog[5]);
//							keyText.set(sViewLog[1]+"|"+sViewLog[0]+"|"+sViewLog[1]+"|"+sViewLog[2]+"|"+sViewLog[3]+"|"+sSchedule[4]);
							valueText.set(String.valueOf(((dViewLogStop.getTime()-dScheduleStart.getTime())/1000)));
							context.write(keyText, valueText);
							
						}else if(dViewLogStart.before(dScheduleStart) & dViewLogStop.after(dScheduleStart)
								& dViewLogStop.after(dScheduleStop)){
							keyText.set(sViewLog[1]+"|"+sViewLog[0]+"|"+sViewLog[1]+"|"+sViewLog[2]+"|"+sViewLog[3]+"|"+sViewLog[4]+"|"+sViewLog[5]);
							valueText.set(String.valueOf(((dScheduleStop.getTime()-dScheduleStart.getTime())/1000)));
							if(Integer.valueOf(valueText.toString())<20000){
								context.write(keyText, valueText);
							}
							
						}else if(dViewLogStart.after(dScheduleStart) & dViewLogStop.after(dScheduleStop)
								& dViewLogStart.before(dScheduleStop)){
							keyText.set(sViewLog[1]+"|"+sViewLog[0]+"|"+sViewLog[1]+"|"+sViewLog[2]+"|"+sViewLog[3]+"|"+sViewLog[4]+"|"+sViewLog[5]);
							valueText.set(String.valueOf(((dScheduleStop.getTime()-dViewLogStart.getTime())/1000)));
							if(Integer.valueOf(valueText.toString())<20000){
								context.write(keyText, valueText);
							}
							
						}else if(dViewLogStart.after(dScheduleStart) & dViewLogStop.before(dScheduleStop)){
							keyText.set(sViewLog[1]+"|"+sViewLog[0]+"|"+sViewLog[1]+"|"+sViewLog[2]+"|"+sViewLog[3]+"|"+sViewLog[4]+"|"+sViewLog[5]);
							valueText.set(sViewLog[5]);
							if(Integer.valueOf(valueText.toString())<20000){
								context.write(keyText, valueText);
							}
							
						}
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}
}

