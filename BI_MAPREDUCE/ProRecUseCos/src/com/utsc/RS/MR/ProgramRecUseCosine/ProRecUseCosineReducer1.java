package com.utsc.RS.MR.ProgramRecUseCosine;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ProRecUseCosineReducer1 extends Reducer<Text, Text, Text, Text>{
	
	Text valueText = new Text();
	private Text keyText = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//		System.out.println("Reduce1 Starting");
//		int count = 0;
		for (Text value : values){
			valueText.set(value);
//			System.out.println("R1 value= "+value);
//			break;
//			count += 1;
//			if(count>1){
//				System.out.println("value= "+value);
//			}
		}
		
		keyText.set(key);
		context.write(keyText, valueText);
//		System.out.println("Reduce1 Finished");
	}

}
