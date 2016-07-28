package com.utsc.RS.MR.ProgramRecUseCosine;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ProRecUseCosineReducer5 extends Reducer<Text, Text, Text, Text> {

	Text valueText = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		SimpleDateFormat dFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date updateTime = new Date();
		String temp = "";
		for (Text value : values){
			temp = value.toString();
		}
		keyText.set(temp);
		valueText.set(dFormat.format(updateTime).toString());
		context.write(keyText, valueText);
		
//		System.out.println("Reduce5 Starting");
//		Comparator<Record> comparator = new Comparator<Record>(){
//			@Override
//			public int compare(Record r1, Record r2) {
//				if(r1.iScore > r2.iScore){
//					return -1;
//				}else if(r1.iScore == r2.iScore){
//					return 0;
//				}else {
//					return 1;
//				}
//			}
//		};
//		ArrayList<Record> Score = new ArrayList<Record>();
//		for (Text value : values) {
//			String [] str= value.toString().trim().split("###");
////			Record record = new Record(Integer.valueOf(str[0]),str[3]+"|"+str[4]+"|"+str[5]+"|"+str[6]+"|"
////					+str[7]+"|"+str[8]+"|"+str[9]+"|"+str[10]+"|"+str[11]+"|"+str[12]+"|"+str[13]+"|"+str[14]);
//			Record record = new Record(Integer.valueOf(str[0].trim()),str[1].trim());
//			Score.add(record);
//		}
//		Collections.sort(Score,comparator);		//sorting the arraylist
//		
////		System.out.println("hahah");
//		SimpleDateFormat dFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		Date updateTime = new Date();
//		for (int i=0;i<Score.size();i++){
////			String[] str = Score.get(i).sString.trim().split("\\|",-1);
//			keyText.set(Score.get(i).sString.trim().trim());
//			//keyText = ViewlogStringUtil.transform(keyText, "GBK");
////			valueText.set(dFormat.format(updateTime).toString()+".0");
//			valueText.set(dFormat.format(updateTime).toString());
//			//valueText = ViewlogStringUtil.transform(valueText, "GBK");
////			System.out.println("time= "+dFormat.format(updateTime).toString()+".0");
//			context.write(keyText, valueText);
//		}
//		System.out.println("Reduce4 Finished");
	}
	
//	class Record{
//		int iScore;
//		String sString;
//		public Record(int iScore, String sString) {
//			this.iScore = iScore;
//			this.sString = sString;
//		}
//	}
}