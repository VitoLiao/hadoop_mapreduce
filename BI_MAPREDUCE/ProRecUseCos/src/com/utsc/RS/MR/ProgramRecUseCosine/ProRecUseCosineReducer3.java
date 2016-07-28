package com.utsc.RS.MR.ProgramRecUseCosine;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ProRecUseCosineReducer3 extends Reducer<Text, Text, Text, Text> {

	Text valueText = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//		System.out.println("Reduce3 Starting");
		keyText.set(key);
		for(Text value : values){
			valueText.set(value);
			context.write(keyText, valueText);
		}
//		System.out.println("Reduce3 Finished");
	}
}