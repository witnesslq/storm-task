package storm.task.constant;

import java.text.SimpleDateFormat;
import java.util.Locale;

public enum SimpleDateFormatEnum {
 	timeFormat("yyyyMMdd HH:mm:ss"),
	dateFormat("yyyyMMdd"),
	dateFormat2("yyyy-MM-dd"),
	monthFormat("yyyyMM"),
	logTimeFormat("yyyyMMddHHmmss"),
	serverTimeFormat("dd/MMM/yyyy:HH:mm:ss",Locale.US),
	hourFormat("yyyy-MM-ddHH"),
	logTimeFormat2("yyyy-MM-dd HH:mm:ss");
 	
	
	String formatStr = null;
	Locale locale = null;
	private SimpleDateFormatEnum(String formatStr) {
		this.formatStr = formatStr;
	}
	private SimpleDateFormatEnum(String formatStr, Locale locale) {
		this.formatStr = formatStr;
		this.locale = locale;
	}
	
	public SimpleDateFormat get(){
		return null == locale ? new SimpleDateFormat(formatStr) : new SimpleDateFormat(formatStr, locale);
	}
}
