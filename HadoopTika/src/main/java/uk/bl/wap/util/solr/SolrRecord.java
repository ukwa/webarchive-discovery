package uk.bl.wap.util.solr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.httpclient.URIException;
import org.apache.hadoop.io.Writable;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;

public class SolrRecord implements Writable {

	/*
	 * Record looks like this:
	 * 
	 * <doc>
	 * <id>1</id>
	 * <title>Iris Murdoch</title>
	 * <subject />
	 * <description />
	 * <comments />
	 * <author />
	 * <keywords />
	 * <category />
	 * <contentType>text/html</contentType>
	 * <timestamp />
	 * <referrerUrl />
	 * <wctInstanceId>29458556</wctInstanceId>
	 * <wctTargetId />
	 * <wctCollections>Blogs,Cornwall</wctCollections>
	 * <wctSubjects />
	 * <wctDescription />
	 * <wctTitle />
	 * <wctUrl>http://www.im.com/index.html</wctUrl>
	 * <wctWaybackDate>20041212180624</wctWaybackDate>
	 * <wctHarvestDate>2004-12-12T23:59:50Z</wctHarvestDate>
	 * <wctAgency><![CDATA[British Library]]></wctAgency>
	 * <wctGovernmentSite>false</wctGovernmentSite>
	 * <text>Iris Murdoch Liberal Democrats...of the site.</text>
	 * </doc>
	 */

	private String id = "";
	private String hash = "";

	private String title = "";
	private String subject = "";
	private String description = "";
	private String comments = "";
	private String author = "";
	private String keywords = "";
	private String category = "";
	private String contentType = "";
	private String timestamp = "";
	private String referrerUrl = "";
	private String wctInstanceId = "";
	private String wctTargetId = "";

	// Can be multiple
	private ArrayList<String> wctCollections = new ArrayList<String>();
	private ArrayList<String> wctSubjects = new ArrayList<String>();

	private String wctDescription = "";
	private String wctTitle = "";
	private String wctUrl = "";
	private String domain = "";
	private String wctWaybackDate = ""; // wayback format
	private String wctHarvestDate = "";
	private String wctAgency = "";
	private String wctGovernmentSite = "";
	private String extractedText = "";
	private String tikaMetadata = "";

	AggressiveUrlCanonicalizer canon = new AggressiveUrlCanonicalizer();

	public static void main( String arg[] ) {
		SolrRecord sr = new SolrRecord();
		sr.setId( "1" );
		sr.setHash( "ABCDEFG" );
		sr.setTitle( "Iris Murdoch" );
		sr.setExtractedText( "Iris Murdoch Liberal Democrats : News HOME NEWS THE PARTY CAMPAIGNS CONFERENCES PEOPLE PARLIAMENT INTERNATIONAL IN YOUR AREA JOIN / SUPPORT GET EMAIL NEWS" );
		sr.setWctCollections( "Collection One" );
		sr.setWctCollections( "Collection Two" );
		sr.setWctCollections( "Collection Three" );
		sr.setWctSubjects( "Subject One" );
		sr.setWctSubjects( "Subject two" );
		sr.setWctSubjects( "Subject three" );
		sr.setWctSubjects( "Subject four" );

		try {
			DataOutput out = new DataOutputStream( new FileOutputStream( "c:\\work\\tmp.dat" ) );
			sr.write( out );

		} catch( Exception e ) {
			e.printStackTrace();
		}
		SolrRecord sr2 = new SolrRecord();

		try {
			DataInput in = new DataInputStream( new FileInputStream( "c:\\work\\tmp.dat" ) );
			sr2.readFields( in );
		} catch( Exception e ) {
			e.printStackTrace();
		}
		System.out.println( sr2.toXml() );

	}

	public String toXml() {
		StringBuilder sb = new StringBuilder();
		sb.append( "<doc>" );
		sb.append( "<id><![CDATA[" + id + "]]></id>" );
		sb.append( "<hash><![CDATA[" + hash + "]]></hash>" );
		sb.append( "<title><![CDATA[" + title + "]]></title>" );
		sb.append( "<subject><![CDATA[" + subject + "]]></subject>" );
		sb.append( "<description><![CDATA[" + description + "]]></description>" );
		sb.append( "<comments><![CDATA[" + comments + "]]></comments>" );
		sb.append( "<author><![CDATA[" + author + "]]></author>" );
		sb.append( "<keywords><![CDATA[" + keywords + "]]></keywords>" );
		sb.append( "<category><![CDATA[" + category + "]]></category>" );
		sb.append( "<contentType><![CDATA[" + contentType + "]]></contentType>" );
		sb.append( "<timestamp><![CDATA[" + timestamp + "]]></timestamp>" );
		sb.append( "<referrerUrl><![CDATA[" + referrerUrl + "]]></referrerUrl>" );
		sb.append( "<wctInstanceId><![CDATA[" + wctInstanceId + "]]></wctInstanceId>" );
		sb.append( "<wctTargetId><![CDATA[" + wctTargetId + "]]></wctTargetId>" );

		Iterator<String> i = wctCollections.iterator();
		while( i.hasNext() ) {
			sb.append( "<wctCollections><![CDATA[" + ( String ) i.next() + "]]></wctCollections>" );
		}
		i = wctSubjects.iterator();
		while( i.hasNext() ) {
			sb.append( "<wctSubjects><![CDATA[" + ( String ) i.next() + "]]></wctSubjects>" );
		}

		sb.append( "<wctDescription><![CDATA[" + wctDescription + "]]></wctDescription>" );
		sb.append( "<wctTitle><![CDATA[" + wctTitle + "]]></wctTitle>" );
		sb.append( "<wctUrl><![CDATA[" + wctUrl + "]]></wctUrl>" );
		sb.append( "<domain><![CDATA[" + domain + "]]></domain>" );
		sb.append( "<wctWaybackDate><![CDATA[" + wctWaybackDate + "]]></wctWaybackDate>" );
		sb.append( "<wctHarvestDate><![CDATA[" + wctHarvestDate + "]]></wctHarvestDate>" );
		sb.append( "<wctAgency><![CDATA[" + wctAgency + "]]></wctAgency>" );
		sb.append( "<wctGovernmentSite><![CDATA[" + wctGovernmentSite + "]]></wctGovernmentSite>" );
		sb.append( "<text><![CDATA[" + extractedText + "]]></text>" );
		sb.append( "<tikaMetadata>" + tikaMetadata + "</tikaMetadata>" );
		sb.append( "</doc>\n" );

		return sb.toString();
	}

	public String getId() {
		return id;
	}

	public void setId( String id ) {
		this.id = id;
	}

	public String getHash() {
		return hash;
	}

	public void setHash( String hash ) {
		this.hash = hash;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle( String title ) {
		this.title = title;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject( String subject ) {
		this.subject = subject;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription( String description ) {
		this.description = description;
	}

	public String getComments() {
		return comments;
	}

	public void setComments( String comments ) {
		this.comments = comments;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor( String author ) {
		this.author = author;
	}

	public String getKeywords() {
		return keywords;
	}

	public void setKeywords( String keywords ) {
		this.keywords = keywords;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory( String category ) {
		this.category = category;
	}

	public String getContentType() {
		return contentType;
	}

	public void setContentType( String contentType ) {
		if( ( contentType.contains( ";" ) ) ) {
			this.contentType = contentType.substring( 0, contentType.indexOf( ";" ) );
		} else {
			this.contentType = contentType;
		}
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp( String timestamp ) {
		this.timestamp = timestamp;
	}

	public String getReferrerUrl() {
		return this.referrerUrl;
	}

	public void setReferrerUrl( String referrerUrl ) {
		this.referrerUrl = referrerUrl;
	}

	public String getWctInstanceId() {
		return wctInstanceId;
	}

	public void setWctInstanceId( String wctInstanceId ) {
		this.wctInstanceId = wctInstanceId;
	}

	public String getWctTargetId() {
		return wctTargetId;
	}

	public void setWctTargetId( String wctTargetId ) {
		this.wctTargetId = wctTargetId;
	}

	public ArrayList<String> getWctCollections() {
		return wctCollections;
	}

	public void setWctCollections( String wctCollections ) {
		this.wctCollections.add( wctCollections );
	}

	public void setWctCollections( ArrayList<String> wctCollections ) {
		this.wctCollections = wctCollections;
	}

	public ArrayList<String> getWctSubjects() {
		return wctSubjects;
	}

	public void setWctSubjects( String wctSubjects ) {
		this.wctSubjects.add( wctSubjects );
	}

	public void setWctSubjects( ArrayList<String> wctSubjects ) {
		this.wctSubjects = wctSubjects;
	}

	public String getWctDescription() {
		return wctDescription;
	}

	public void setWctDescription( String wctDescription ) {
		this.wctDescription = wctDescription;
	}

	public String getWctTitle() {
		return wctTitle;
	}

	public void setWctTitle( String wctTitle ) {
		this.wctTitle = wctTitle;
	}

	public String getWctUrl() {
		return wctUrl;
	}

	public void setWctUrl( String wctUrl ) throws URIException {
		this.wctUrl = wctUrl;
		this.domain = canon.urlStringToKey( wctUrl ).split( "/" )[ 0 ];
	}

	public String getDomain() {
		return this.domain;
	}

	public String getWctWaybackDate() {
		return wctWaybackDate;
	}

	public void setWctWaybackDate( String wctWaybackDate ) {
		this.wctWaybackDate = wctWaybackDate;
	}

	public String getWctHarvestDate() {
		return wctHarvestDate;
	}

	public String getWctAgency() {
		return wctAgency;
	}

	public void setWctAgency( String wctAgency ) {
		this.wctAgency = wctAgency;
	}

	public void setWctHarvestDate( String wctHarvestDate ) {
		this.wctHarvestDate = wctHarvestDate;
	}

	public String getWctGovernmentSite() {
		return wctGovernmentSite;
	}

	public void setWctGovernmentSite( String wctGovernmentSite ) {
		this.wctGovernmentSite = wctGovernmentSite;
	}

	public String getExtractedText() {
		return extractedText;
	}

	public void setExtractedText( String extractedText ) {
		this.extractedText = extractedText;
	}

	public String getTikaMetadata() {
		return tikaMetadata;
	}

	public void setTikaMetadata( String tikaMetadata ) {
		this.tikaMetadata = tikaMetadata;
	}

	public void write( DataOutput out ) throws IOException {
		writeBytes( out, id );
		writeBytes( out, hash );
		writeBytes( out, title );
		writeBytes( out, subject );
		writeBytes( out, description );
		writeBytes( out, comments );
		writeBytes( out, author );
		writeBytes( out, keywords );
		writeBytes( out, category );
		writeBytes( out, contentType );
		writeBytes( out, timestamp );
		writeBytes( out, referrerUrl );
		writeBytes( out, wctInstanceId );
		writeBytes( out, wctTargetId );
		writeRecord( out, wctCollections );
		writeRecord( out, wctSubjects );
		writeBytes( out, wctDescription );
		writeBytes( out, wctTitle );
		writeBytes( out, wctUrl );
		writeBytes( out, domain );
		writeBytes( out, wctWaybackDate );
		writeBytes( out, wctHarvestDate );
		writeBytes( out, wctAgency );
		writeBytes( out, wctGovernmentSite );
		writeBytes( out, extractedText );
		writeBytes( out, tikaMetadata );
	}

	public void writeBytes( DataOutput o, String field ) throws IOException {
		if( field == null ) {
			o.writeInt( 0 );
		} else {
			o.writeInt( field.getBytes().length );
			o.write( field.getBytes() );
		}
	}

	public void writeRecord( DataOutput o, ArrayList<String> field ) throws IOException {
		int count = field.size();
		o.writeInt( count );
		Iterator<String> i = field.iterator();
		while( i.hasNext() ) {
			writeBytes( o, ( String ) i.next() );
		}
	}

	public String readBytes( DataInput i ) throws IOException {
		int length = i.readInt();
		byte[] b = new byte[ length ];
		i.readFully( b );
		return new String( b );
	}

	public ArrayList<String> readRecords( DataInput i ) throws IOException {
		int count = i.readInt();
		ArrayList<String> ls = new ArrayList<String>();
		for( int x = 0; x < count; x++ ) {
			ls.add( readBytes( i ) );
		}
		return ls;
	}

	public void readFields( DataInput in ) throws IOException {
		id = readBytes( in );
		hash = readBytes( in );
		title = readBytes( in );
		subject = readBytes( in );
		description = readBytes( in );
		comments = readBytes( in );
		author = readBytes( in );
		keywords = readBytes( in );
		category = readBytes( in );
		contentType = readBytes( in );
		timestamp = readBytes( in );
		referrerUrl = readBytes( in );
		wctInstanceId = readBytes( in );
		wctTargetId = readBytes( in );

		// Read potentially many lines
		wctCollections = readRecords( in );
		wctSubjects = readRecords( in );

		wctDescription = readBytes( in );
		wctTitle = readBytes( in );
		wctUrl = readBytes( in );
		domain = readBytes( in );
		wctWaybackDate = readBytes( in );
		wctHarvestDate = readBytes( in );
		wctAgency = readBytes( in );
		wctGovernmentSite = readBytes( in );
		extractedText = readBytes( in );
		tikaMetadata = readBytes( in );
	}
}