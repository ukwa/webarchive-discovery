/**
 * 
 */
package uk.bl.wa.tika;

import org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;

/**
 * Override embedded document parser logic to prevent descent.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 * 
 */
public class NonRecursiveEmbeddedDocumentExtractor extends
		ParsingEmbeddedDocumentExtractor {

	/** Parse embedded documents? Defaults to TRUE */
	private boolean parseEmbedded = true;

	public NonRecursiveEmbeddedDocumentExtractor(ParseContext context) {
		super(context);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor#
	 * shouldParseEmbedded(org.apache.tika.metadata.Metadata)
	 */
	@Override
	public boolean shouldParseEmbedded(Metadata metadata) {
		return this.parseEmbedded;
	}

	/**
	 * @return the parseEmbedded
	 */
	public boolean isParseEmbedded() {
		return parseEmbedded;
	}

	/**
	 * @param parseEmbedded
	 *            the parseEmbedded to set
	 */
	public void setParseEmbedded(boolean parseEmbedded) {
		this.parseEmbedded = parseEmbedded;
	}

}