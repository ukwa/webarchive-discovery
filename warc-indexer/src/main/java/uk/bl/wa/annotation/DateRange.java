package uk.bl.wa.annotation;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2021 The webarchive-discovery project contributors
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import com.fasterxml.jackson.annotation.JsonProperty;

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

    public Date getStart() {
        if (this.start == null) {
            return new Date(0L);
        } else {
            return start;
        }
    }

    public Date getEnd() {
        if (this.end == null) {
            return getDistantFutureDate();
        } else {
            return end;
        }
    }

    public boolean isInDateRange(Date date) {
        // System.err.println("isInDateRange "
        // + (date.after(getStart()) && date.before(getEnd())));
        return (date.after(getStart()) && date.before(getEnd()));
    }

    public String toString() {
        return "[" + start + "," + end + "]";
    }

    private Date getDistantFutureDate() {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, 9999);
        calendar.set(Calendar.MONTH, Calendar.DECEMBER);
        calendar.set(Calendar.DAY_OF_MONTH, 30);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        calendar.set(Calendar.MILLISECOND, 999);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        return calendar.getTime();
    }
}
