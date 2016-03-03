package com.viewlog.util;

import java.io.UnsupportedEncodingException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

public class ViewlogStringUtil {
	public static boolean isNumeric(String str){
	    Pattern pattern = Pattern.compile("[0-9]*");
	    return pattern.matcher(str).matches();   
	 } 
	
	public static Text transform(Text text, String encoding) {
		String value = null;
		try {
			value = new String(text.getBytes(), 0, text.getLength(), encoding);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return new Text(value);
	}	
}
