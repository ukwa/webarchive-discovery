package uk.bl.wa.solr;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
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

public interface WctFields {

    public static final String WCT_INSTANCE_ID = "wct_instance_id";
    public static final String WCT_TARGET_ID = "wct_target_id";
    public static final String WCT_COLLECTIONS = "wct_collections";
    public static final String WCT_SUBJECTS = "wct_subjects";
    public static final String WCT_DESCRIPTION = "wct_description";
    public static final String WCT_TITLE = "wct_title";
    public static final String WCT_WAYBACK_DATE = "wct_wayback_date";
    public static final String WCT_HARVEST_DATE = "wct_harvest_date";
    public static final String WCT_AGENCY = "wct_agency";
    public static final String WCT_GOVERNMENT_SITE = "wct_government_site";
}
