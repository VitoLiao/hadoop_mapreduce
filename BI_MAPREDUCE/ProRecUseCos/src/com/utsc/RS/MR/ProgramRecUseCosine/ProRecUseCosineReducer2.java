package com.utsc.RS.MR.ProgramRecUseCosine;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ProRecUseCosineReducer2 extends Reducer<Text, Text, Text, Text> {

	Text valueText = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//		System.out.println("Reduce2 Starting");
		// input
		// key value
		// ROOTCATCODES|ROOTCATNAMES|HEFLAG
		// |C2CODE|DIRECTORS|CASTS|SPLIT_WORD|YEAR|GENRES|COUNTRIES|LANGUAGES|WRITERS
		// |RATING|IMAGE|SEQID|NAME|TYPE|ROOTCATCODES|ROOTCATNAMES|HDFLAG|forcedrootcatcode|forcedrootcatname
		// output
		// key value
		// value(i)	values
		
		ArrayList<String> aList = new ArrayList<>();
		String aALL = "";

		for (Text value : values) {
			aList.add(value.toString());
		}
		int len = aList.size();
		aALL = String.join("##", aList);
		int roundsize = Integer.valueOf(context.getConfiguration().get("rownum"));
		StringBuffer sRemain = new StringBuffer();
		if (len<roundsize){
			sRemain.append("0");
			for(int i=1;i<len;i++){
				sRemain.append("##"+i);
			}
//			keyText.set(aALL+"###"+sRemain);
			keyText.set(aALL);
			valueText.set(sRemain.toString());
			context.write(keyText, valueText);
//			System.out.println("R21 valuetext= "+valueText.toString());
		}else{
			int roundNum = (int)Math.ceil((double)len/(double)roundsize);
//			int k=0;
			for(int i=0;i<roundNum;i++){
				if(i==roundNum-1){
					sRemain = new StringBuffer();
					sRemain.append(roundsize*(roundNum-1));
					for(int j=roundsize*(roundNum-1)+1;j<len;j++){
						sRemain.append("##"+j);
					}
					keyText.set(aALL);
					valueText.set(sRemain.toString());
					context.write(keyText, valueText);
//					System.out.println("R22 valuetext= "+valueText.toString());
//					k += 1;
				}else {
					sRemain = new StringBuffer();
					sRemain.append(roundsize*i);
					for(int j=roundsize*i+1;j<roundsize*(i+1);j++){
						sRemain.append("##"+j);
					}
					keyText.set(aALL);
					valueText.set(sRemain.toString());
					context.write(keyText, valueText);
//					System.out.println("R23 valuetext= "+valueText.toString());
//					k += 1;
				}
//				System.out.println("k= "+k);
			}
		}
//		System.out.println("Reduce1 Finished");
	}
}