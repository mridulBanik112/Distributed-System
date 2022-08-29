package cs455.aqi;
import java.util.Calendar;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.IsoFields;
import java.util.Date;
import java.util.TimeZone;

public class DateUtility {

	// Epoch time to yyyy.
	public static String epochTimeToYear(long et) {
		Date date = new Date(et);
		DateFormat format = new SimpleDateFormat("yyyy");
		format.setTimeZone(TimeZone.getTimeZone("UTC"));
		return format.format(date);
	}

	// Epoch time to MM/dd/yyyy.
	public static String epochTimeToDate(long et) {
		Date date = new Date(et);
		DateFormat format = new SimpleDateFormat("MM/dd/yyyy");
		format.setTimeZone(TimeZone.getTimeZone("UTC"));
		return format.format(date);
	}

	// Epoch time to w (week).
	public static String epochTimeToWeek(long et) {
		LocalDateTime ldt = LocalDateTime.ofEpochSecond(et, 0, ZoneOffset.UTC);
		int wk = ldt.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
		return String.valueOf(wk);
	}
	
	// Epoch time to month.
	public static String epochTimeToMonth(long et) {
		Date date = new Date(et);
		DateFormat format = new SimpleDateFormat("MM");
		format.setTimeZone(TimeZone.getTimeZone("UTC"));
		return format.format(date);
	}
	// Epoch time to Day
	public static int epochTimeToDay(long et) {
		Date date = new Date(et);
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
		return dayOfWeek;
	}
	
}
