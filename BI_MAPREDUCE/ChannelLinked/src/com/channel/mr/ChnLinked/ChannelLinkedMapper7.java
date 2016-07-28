package com.channel.mr.ChnLinked;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChannelLinkedMapper7 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// input
		// PACKAGECODE/PROGRAMCODE##...###
		// ot|DATE|CONTENTTYPE|TYPE|CODE|USERID|TIMEINTERVAL
		
		// output
		// DATE|CONTENTTYPE|TYPE|CODE|PACKAGECODE/PROGRAMCODE| USERID|TIMEINTERVAL|LINKTYPE
		if(value.getLength()>0){
			String[] strings = value.toString().split("###",-1);
			String[] aStrings = strings[0].split("##");
			String[] bStrings = strings[1].split("##");
			String linktype = "4";
			for(int i = 0 ; i<aStrings.length;i++){
				for (int j =0 ;j<bStrings.length;j++){
					String[] VL = bStrings[j].split("\\|",-1);
					keyText.set(VL[1]+"|"+VL[2]+"|"+VL[3]
							+"|"+VL[4]+"|"+aStrings[i]);
					valueText.set(VL[5]+"|"+VL[6]+"|"+linktype);
					context.write(keyText, valueText);
				}
			}
		}
	}
}

