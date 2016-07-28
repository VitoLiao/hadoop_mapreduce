package com.channel.mr.ChnLinked;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ChannelLinkedReducer1 extends Reducer<Text, Text, Text, Text>{
	
	Text valueText = new Text();
	private Text keyText = new Text();
	
	private MultipleOutputs<Text, Text> multipleOutputs;

	protected void setup(Context context) throws IOException, InterruptedException {
		multipleOutputs = new MultipleOutputs<>(context);
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// c|DATE|CONTENTTYPE|TYPE|CODE|USERID|TIMEINTERVAL
		keyText.set(key);
		int TIMEINTERVAL = 0;
		String[] str = key.toString().split("\\|",-1);
		for(Text value : values){
			if(str[0].equals("c")){
				TIMEINTERVAL += Integer.valueOf(value.toString());
			}else if(str[0].equals("ct")){
				TIMEINTERVAL += Integer.valueOf(value.toString());
			}else if(str[0].equals("t")){
				TIMEINTERVAL += Integer.valueOf(value.toString());
			}else if(str[0].equals("ot")){
				valueText.set(value);
				multipleOutputs.write(keyText, valueText, "ot"+"/part");
			}
		}
		
		if(str[0].equals("c")){
			valueText.set(String.valueOf(TIMEINTERVAL));
			multipleOutputs.write(keyText, valueText, "c"+"/part");
		}else if(str[0].equals("ct")){
			valueText.set(String.valueOf(TIMEINTERVAL));
			multipleOutputs.write(keyText, valueText, "ct"+"/part");
		}else if(str[0].equals("t")){
			valueText.set(String.valueOf(TIMEINTERVAL));
			multipleOutputs.write(keyText, valueText, "t"+"/part");
		}else if(str[0].equals("ot")){
		}
	}
}
