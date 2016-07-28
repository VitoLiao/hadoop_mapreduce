package com.utsc.RS.MR.ProgramRecUseCosine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import sun.net.www.content.text.plain;

public class ProRecUseCosineMapper5 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// System.out.println("Map5 Starting");
		// input
		// key value
		//
		// SEQIDa|C2CODEa|SEQIDb|C2CODEb|NAMEa|NAMEb|RATINGb|ROOTCATCODESb|ROOTCATNAMESb|SCORE|IMAGEb|COMMENTb|HDFLAGb|TYPE
		// output
		// key value
		// C2CODEa|SEQIDa|C2CODEb|SEQIDb correlationscore
		if (value.toString().length() > 0) {

			String[] strings = value.toString().trim().split("###", -1);
			String[] str = strings[2].trim().split("\\|", -1);
			String aString = str[1].trim();
			String bString = str[3].trim();
			
			if(str[4].trim().equals(str[5].trim())){
				
			}else {
				keyText.set(aString + "|" + bString);
				valueText.set(str[0] + "|" + str[1] + "|" + str[2] + "|" + str[3] + "|" + str[4] + "|" + str[5]
						+ "|" + str[6] + "|" + str[7] + "|" + str[8] + "|" + str[9] + "|" + str[10] + "|" + str[11]
						+ "|" + str[12] + "|" + str[13]);
				context.write(keyText, valueText);
			}
			
			
//			if (aString.length() > 4) {
//				boolean aFlag = false;
//				boolean bFlag = false;
//				if (!aString.equals(bString)) {
//
//					if (str[4].trim().substring(1, 3).equals("HD")) {
//						aString = str[4].trim().substring(4, str[4].trim().length());
//						aFlag = true;
//					} else if (str[4].trim().substring(1, 3).equals("BR")) {
//						aString = str[4].trim().substring(4, str[4].trim().length());
//						aFlag = true;
//					} else if (str[4].trim().substring(0, 2).equals("HD")) {
//						aString = str[4].trim().substring(2, str[4].trim().length());
//						aFlag = true;
//					} else if (str[4].trim().substring(0, 2).equals("BR")) {
//						aString = str[4].trim().substring(2, str[4].trim().length());
//						aFlag = true;
//					}
//				}
//				if (bString.length() > 4) {
//					if (str[5].trim().substring(1, 3).equals("HD")) {
//						bString = str[5].trim().substring(4, str[5].trim().length());
//						bFlag = true;
//					} else if (str[5].trim().substring(1, 3).equals("BR")) {
//						bString = str[5].trim().substring(4, str[5].trim().length());
//						bFlag = true;
//					} else if (str[5].trim().substring(0, 2).equals("HD")) {
//						bString = str[5].trim().substring(2, str[5].trim().length());
//						bFlag = true;
//					} else if (str[5].trim().substring(0, 2).equals("BR")) {
//						bString = str[5].trim().substring(2, str[5].trim().length());
//						bFlag = true;
//					}
//				}
//
//				if (aFlag) {
//					keyText.set(aString + "|" + bString + "1");
//					valueText.set(str[0] + "|" + str[1] + "|" + str[2] + "|" + str[3] + "|" + str[4] + "|" + str[5]
//							+ "|" + str[6] + "|" + str[7] + "|" + str[8] + "|" + str[9] + "|" + str[10] + "|" + str[11]
//							+ "|" + str[12] + "|" + str[13]);
//					context.write(keyText, valueText);
//				} else if (!aFlag) {
//					if (!bFlag) {
//						keyText.set(aString + "|" + bString + "0");
//						valueText.set(str[0] + "|" + str[1] + "|" + str[2] + "|" + str[3] + "|" + str[4] + "|" + str[5]
//								+ "|" + str[6] + "|" + str[7] + "|" + str[8] + "|" + str[9] + "|" + str[10] + "|"
//								+ str[11] + "|" + str[12] + "|" + str[13]);
//						context.write(keyText, valueText);
//					}
//				}
//			}

			// String [] strings=value.toString().trim().split("###",-1);
			// String [] str= strings[2].trim().split("\\|",-1);
			//
			// keyText.set(str[1]+"|"+str[3]);
			// valueText.set(str[0]+"|"+str[1]+"|"+str[2]+"|"+str[3]
			// +"|"+str[4]+"|"+str[5]+"|"+ str[6]+"|"+ str[7]+"|"+ str[8]+"|"+
			// str[9]+"|"+ str[10]+"|"+ str[11]+"|"+ str[12]+"|"+ str[13]);
			// context.write(keyText, valueText);
			// System.out.println("keyt= "+keyText.toString());
		}
		// System.out.println("Map4 Finished");

//		 if(value.toString().length()>0){
//		// System.out.println("value= "+value);
//		 String [] str=value.toString().trim().split("###");
//		 keyText.set(str[0].trim());
//		 valueText.set(str[1].trim()+"###"+str[2].trim());
//		 context.write(keyText, valueText);
//		 }

	}
}
