package uk.bl.wa.annotation;

import java.util.Calendar;
import java.util.Date;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * DateRange: holds a start/end date for mapping Collection timeframes.
 * 
 * Part of the @Annotations data model.
 * 
 * @author rcoram
 * 
 */
public class DateRange {

	@JsonProperty
	protected Date start;

	@JsonProperty
	protected Date end;

	protected DateRange() {
	}

	public DateRange(String start, String end) {
		if (start != null)
			this.start = new Date(Long.parseLong(start) * 1000L);
		else
			this.start = new Date(0L);

		if (end != null)
			this.end = new Date(Long.parseLong(end) * 1000L);
		else {
			this.end = getDistantFutureDate();
		}
	}

	public boolean isInDateRange(Date date) {
		return (date.after(start) && date.before(end));
	}

	public String toString() {
		return "[" + start + "," + end + "]";
	}

	private Date getDistantFutureDate() {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, 9999);
		return calendar.getTime();
	}
}