package com.utsc.RS.MR.ProgramRecUseCosine;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProRecUseCosineMapper1 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// System.out.println("M1 starting");
		// keyText.set(key.toString());
		// valueText.set(value);
		// context.write(keyText, valueText);
		// output
		// key value
		// ROOTCATCODES|ROOTCATNAMES|C2CODE|SEQID|NAME|HDFLAG
		// DIRECTORS|CASTS|SPLIT_WORD|YEAR|GENRES|COUNTRIES|LANGUAGES|WRITERS
		// |RATING|IMAGE|TYPE|LEAFCATCODES|LEAFCATNAMES
		if (value.getLength() > 0) {
			// System.out.println("value "+value);
			Text line = transformTextToUTF8(value, "GBK");
			String[] str = line.toString().trim().split("\\|", -1);
			// System.out.println("line= "+line);
			// 02000001000000090000000000036300
			if (str.length >= 25) {
				// if (str[24].equals("0")) {
				//
				// } else {

				Pattern pattern = Pattern.compile("[0-9]*");
				Matcher isNum = pattern.matcher(str[5].trim());
				if (isNum.matches()) {
					if (str[5].trim().length() > 4) {
						str[5] = str[5].trim().substring(0, 4);
					}
				} else {
					str[5] = " ";
				}
				if (str[24].trim().length() > 0) {
					// if(str[13].equals("02000001000000090000000000036298")){

					// }
					if (str[24].trim().equals("02000001000000090000000000036294")) {
						// System.out.println("news");
						// } else if
						// (str[24].trim().equals("02000001000000090000000000036298"))
						// {
					} else {
						if (str[22].trim().equals("9")) {
							// System.out.println("9");
						} else if (str[22].trim().equals("4")) {
							// System.out.println("4");
						} else {
							// System.out.println("line= " + str[22]);
							// }
							// System.out.println("M1");
							// System.out.println("value= "+line.toString());
							// System.out.println("image= "+str[22]+"
							// "+str[23]+"
							// "+str[24]);

							keyText.set(str[24].trim() + "|" + str[25].trim() + "|" + str[13].trim() + "|"
									+ str[21].trim() + "|" + str[14].trim() + "|" + str[26].trim());

							valueText.set(str[2].trim() + "|" + str[3].trim() + "|" + str[11].trim() + "|"
									+ str[5].trim() + "|" + str[8].trim() + "|" + str[9].trim() + "|" + str[6].trim()
									+ "|" + str[4].trim() + "|" + str[1].trim() + "|" + str[23].trim() + "|"
									+ str[12].trim() + "|" + str[27] + "|" + str[28]);
							// System.out.println(keyText+"|"+valueText);
							context.write(keyText, valueText);
						}
					}
				}
			}
		}
	}

	// System.out.println("Map1 Finished");
	// }

	public static Text transformTextToUTF8(Text text, String encoding) {
		String value = null;
		try {
			value = new String(text.getBytes(), 0, text.getLength(), encoding);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return new Text(value);
	}
}
