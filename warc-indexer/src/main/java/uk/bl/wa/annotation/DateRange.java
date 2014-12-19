package uk.bl.wa.annotation;

import java.util.Date;

/**
 * DateRange: holds a start/end date for mapping Collection timeframes.
 * 
 * @author rcoram
 * 
 */
public class DateRange {
	protected Date start;
	protected Date end;

	public DateRange(String start, String end) {
		if (start != null)
			this.start = new Date(Long.parseLong(start) * 1000L);
		else
			this.start = new Date(0L);

		if (end != null)
			this.end = new Date(Long.parseLong(end) * 1000L);
		else
			this.end = new Date(Long.MAX_VALUE);
	}

	public boolean isInDateRange(Date date) {
		return (date.after(start) && date.before(end));
	}
}