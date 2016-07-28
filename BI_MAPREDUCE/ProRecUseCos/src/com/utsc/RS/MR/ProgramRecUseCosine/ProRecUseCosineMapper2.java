package com.utsc.RS.MR.ProgramRecUseCosine;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ProRecUseCosineMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// System.out.println("Map2 Starting");
		// input
		// ROOTCATCODES|ROOTCATNAMES|C2CODE|SEQID|NAME|HEFLAG|DIRECTORS|CASTS|SPLIT_WORD|YEAR|GENRES|COUNTRIES|LANGUAGES|WRITERS
		// |RATING|IMAGE|TYPE|LEAFCATCODES|LEAFCATNAMES
		// output
		// key value
		// ROOTCATCODES|ROOTCATNAMES|HDFLAG
		// C2CODE|DIRECTORS|CASTS|SPLIT_WORD|YEAR|GENRES|COUNTRIES|LANGUAGES|WRITERS|RATING|IMAGE|SEQID|NAME|TYPE
		// |ROOTCATCODES|ROOTCATNAMES|HDFLAG|LEAFCATCODES|LEAFCATNAMES
		// keyText.set(key.toString());
		// valueText.set(value);
		// context.write(keyText, valueText);

		boolean debugflag = false;
		if (debugflag) {
			if (value.getLength() > 0) {
				String[] str = value.toString().trim().split("\\|", -1);
				String tmp = str[0] + "|" + str[1] + "|" + str[5];
				keyText.set(str[5]);
				valueText.set(str[2] + "|" + str[6] + "|" + str[7] + "|" + str[8] + "|" + str[9] + "|" + str[10] + "|"
						+ str[11] + "|" + str[12] + "|" + str[13] + "|" + str[14] + "|" + str[15] + "|" + str[3] + "|"
						+ str[4] + "|" + str[16] + "|" + tmp);
				context.write(keyText, valueText);
				// System.out.println("kv "+keyText+"|"+valueText);
			}
		} else {
			if (value.getLength() > 0) {

				String[] str = value.toString().trim().split("\\|", -1);
//				if (str[15].trim().length() > 0) {
				String tmp = str[0] + "|" + str[1] + "|" + str[5];
				keyText.set(str[0] + "|" + str[1] + "|" + str[5]);
				valueText.set(str[2] + "|" + str[6] + "|" + str[7] + "|" + str[8] + "|" + str[9] + "|" + str[10]
						+ "|" + str[11] + "|" + str[12] + "|" + str[13] + "|" + str[14] + "|" + str[15] + "|"
						+ str[3] + "|" + str[4] + "|" + str[16] + "|" + tmp + "|" + str[17] + "|" + str[18]);
				context.write(keyText, valueText);
				// System.out.println("kv "+keyText+"|"+valueText);

//					System.out.println("image= " + str[15].trim());
//				}
			}
		}
		// System.out.println("Map2 Finished");

	}

}
