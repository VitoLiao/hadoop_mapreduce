package com.utsc.RS.MR.ProgramRecUseCosine;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ProRecUseCosineMapper3 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// System.out.println("Map3 Starting");
		// input
		// key value
		// ROOTCATCODES|ROOTCATNAMES|HEFLAG
		// |C2CODE|DIRECTORS|CASTS|SPLIT_WORD|YEAR|GENRES|COUNTRIES|LANGUAGES|WRITERS
		// |RATING|IMAGE|SEQID|NAME|TYPE|ROOTCATCODES|ROOTCATNAMES|HDFLAG|LEAFCATCODES|LEAFCATNAMES
		// All###1##2##...
		if (value.toString().length() > 0) {
			int hLen = Integer.valueOf(context.getConfiguration().get("hLen"));
			String[] bigboss = value.toString().trim().split("###");
			String[] later = bigboss[1].trim().split("##");
			if (later.length < hLen) {
				keyText.set(bigboss[0].trim());
				valueText.set(bigboss[1].trim());
				context.write(keyText, valueText);
//				System.out.println("valueText1= " + valueText);
			} else {
				int hround = (int) Math.ceil((double) later.length / (double) hLen);
				for (int i = 0; i < hround; i++) {
					StringBuffer hTemp = new StringBuffer();
					if (i == hround - 1) {
						hTemp.append(hLen * i);
						for (int j = hLen * i + 1; j < later.length; j++) {
							hTemp.append("##" + later[j]);
						}
						keyText.set(bigboss[0].trim());
						valueText.set(hTemp.toString());
						context.write(keyText, valueText);
//						 System.out.println("valueText2= " + valueText);
					} else {
						hTemp.append(hLen * i);
						for (int j = hLen * i + 1; j < hLen * (i + 1); j++) {
							hTemp.append("##" + later[j]);
						}
						keyText.set(bigboss[0].trim());
						valueText.set(hTemp.toString());
						context.write(keyText, valueText);
//						 System.out.println("valueText3= " + valueText);
					}
				}
			}
		}
		// System.out.println("Map3 Finished");
	}

}
