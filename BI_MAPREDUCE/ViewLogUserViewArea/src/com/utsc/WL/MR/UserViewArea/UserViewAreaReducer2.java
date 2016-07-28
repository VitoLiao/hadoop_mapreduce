package com.utsc.WL.MR.UserViewArea;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserViewAreaReducer2 extends Reducer<Text, Text, Text, Text> {

	Text valueText = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// input
		// key valut
		// DATE|AreaCode LogType|viewTime|viewInterval or b|DATE|AreaCode UserId
		// c:channel t:backwatch v:pointbrocast

		// a|DATE|AreaCode|LogType	viewInterval
		// b|DATE|AreaCode	UserId
		String[] keyStr = key.toString().split("\\|",-1);
		long viewNumberAll = 0;
		for(Text value : values){
			if(keyStr[0].trim().equals("a")){
				valueText.set(value);
				keyText.set(key);
				context.write(keyText, valueText);
			}else if (keyStr[0].trim().equals("b")) {
				valueText.set(value);
				keyText.set(key);
				context.write(keyText, valueText);
			}else if (keyStr[0].trim().equals("c")) {
				valueText.set(value);
				keyText.set(key);
				context.write(keyText, valueText);
			}else if (keyStr[0].trim().equals("d")) {
				viewNumberAll += 1;
			}else if (keyStr[0].trim().equals("e")) {
				valueText.set(value);
				keyText.set(key);
				context.write(keyText, valueText);
			}
		}
		
		if(keyStr[0].trim().equals("a")){
			
		}else if (keyStr[0].trim().equals("b")) {
			
		}else if (keyStr[0].trim().equals("c")) {
			
		}else if (keyStr[0].trim().equals("d")) {
			keyText.set(key);
			valueText.set(String.valueOf(viewNumberAll));
			context.write(keyText, valueText);
		}else if (keyStr[0].trim().equals("e")) {
		}
	}
}
