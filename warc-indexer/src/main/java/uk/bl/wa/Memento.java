package uk.bl.wa;

/*-
 * #%L
 * warc-hadoop-indexer
 * %%
 * Copyright (C) 2013 - 2022 The webarchive-discovery project contributors
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

import java.io.Serializable;
import java.util.List;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * POJO that declares the information we can know about an archived web resource (a.k.a. Memento)
 * Used to manage/generate a suitable schema, and JSON encoding compatible with Solr.
 */
public class Memento {
    
    private String id;
    
    @JsonProperty("access_terms")
    private String accessTerms; // From annotations
    
    private String author;
    
    private String category; // Not in use
    
    private String collection; // From annotations
    
    private List<String> collections; // From annotations
    
    private List<String> comments; // Not in use
    
    private String description;
    
    private List<String> keywords;
    
    @JsonProperty("license_url")
    private List<String> licenceUrl;
    
    @JsonProperty("content")
    private String contentText; // The extracted text (called 'content' in Solr schema)
    
    @JsonProperty("content_encoding")
    private String contentTextOriginalEncoding; // 'content_encoding' in Solr schema
    
    //private byte[] content_ffb; // First four bytes, output as lower-case hex string. Derive from first bytes.
    
    @JsonProperty("content_first_bytes")
    private byte[] contentFirstBytes; // First 32 bytes, output as space-separated hex.
    
    @JsonProperty("content_language")
    private String contentLanguage;
    
    @JsonProperty("content_length")
    private long contentLength;
    
    // String content_metadata, // Not sure how to store that
    
    @JsonProperty("content_text_length")
    private long contentTextLength;
    
    @JsonProperty("content_type_droid")
    private String contentTypeDroid;
    
    @JsonProperty("content_type_ext")
    private String contentTypeExt;
    
    @JsonProperty("content_type_full")
    private String contentTypeFull;
    
    @JsonProperty("content_type_norm")
    private String contentTypeNorm;
    
    @JsonProperty("content_type_served")
    private String contentTypeServed;
    
    @JsonProperty("content_type_tika")
    private String contentTypeTika;
    
    @JsonProperty("content_type")
    private String contentType;
    
    @JsonProperty("content_type_version")
    private String contentTypeVersion;
    
    @JsonProperty("elements_used")
    private List<String> elementsUsed; // Best thing to be doing?
    
    private String hash;
    
    //List<String> hashes; // Not in use
    
    // long id_long; // Not in use
    
    @JsonProperty("wayback_date")
    private long waybackDate;
    
    // List<Date> crawl_dates; // Not in use
    
    @JsonProperty("crawl_date")
    private Date crawlDate;
    
    // List<int> crawl_years; // Not in use
    
    // int crawl_year; // To be generated from crawl_date;
    
    @JsonProperty("last_modified")
    private Date lastModified;
    
    // int last_modified_year; // To be generated from last_modified
    
    @JsonProperty("url_norm")
    private String urlNorm;
    
    // String url_search; // Generated from url_norm?
    
    @JsonProperty("url_path")
    private String urlPath;
    
    private String url;
    
    @JsonProperty("url_type")
    private String urlType;
    
    private String domain;
    
    private String host;
    
    @JsonProperty("host_surt")
    private List<String> hostSurt;
    
    @JsonProperty("public_suffix")
    private String publicSuffix;
    
    private String resourcename; // Needed?
    
    @JsonProperty("resourcenameFacet")
    private String resourcename_facet; // Needed? Copied from resourcename?
    
    @JsonProperty("image_colours")
    private List<String> imageColours;
    
    @JsonProperty("image_dominant_colour")
    private String imageDominantColour;
    
    @JsonProperty("image_faces_count")
    private int imageFacesCount;
    
    @JsonProperty("image_faces")
    private List<String> imageFaces;
    
    @JsonProperty("image_height")
    private long imageHeight;
    
    @JsonProperty("image_width")
    private long imageWidth;
    
    @JsonProperty("image_size")
    private long imageSize;
    
    @JsonProperty("links_images")
    private List<String> linksImages;
    
    @JsonProperty("links_domains")
    private List<String> linksDomains;
    
    @JsonProperty("links_hosts")
    private List<String> linksHosts;
    
    @JsonProperty("links_hosts_surts")
    private List<String> linksHostsSurts;
    
    @JsonProperty("links_public_suffixes")
    private List<String> linksPublicSuffixes;
    
    private List<String> links;
    
    private List<String> locations;
    
    @JsonProperty("parse_error")
    private List<String> parseErrors;
    
    @JsonProperty("pdf_pdfa_errors")
    private List<String> pdfPdfaErrors;
    
    @JsonProperty("pdf_pdfa_is_valid")
    private String pdfPdfaIsValid;
    
    @JsonProperty("postcode_district")
    private List<String> postcodeDistrict;
    
    private List<String> postcode;
    
    @JsonProperty("publication_date")
    private Date publicationDate;
    
    @JsonProperty("publication_year")
    private int publicationYear;
    
    @JsonProperty("record_type")
    private String recordType;
    
    //float sentiment_score; // Not in use
    
    //String sentiment; // Not in use
    
    private List<String> server;
    
    @JsonProperty("status_code")
    private int statusCode;
    
    private List<String> generator;
    
    //String referrer_url; // Not in use
    
    @JsonProperty("redirect_to_norm")
    private String redirectToNorm;

    @JsonProperty("source_file_path")
    private String sourceFilePath;
    
    @JsonProperty("source_file_offset")
    private long sourceFileOffset;

    @JsonProperty("source_file")
    private String sourceFile;


    public Memento() {
    }


    public String getId() {
        return id;
    }


    public void setId(String id) {
        this.id = id;
    }


    public String getAccessTerms() {
        return accessTerms;
    }


    public void setAccessTerms(String accessTerms) {
        this.accessTerms = accessTerms;
    }


    public String getAuthor() {
        return author;
    }


    public void setAuthor(String author) {
        this.author = author;
    }


    public String getCategory() {
        return category;
    }


    public void setCategory(String category) {
        this.category = category;
    }


    public String getCollection() {
        return collection;
    }


    public void setCollection(String collection) {
        this.collection = collection;
    }


    public List<String> getCollections() {
        return collections;
    }


    public void setCollections(List<String> collections) {
        this.collections = collections;
    }


    public List<String> getComments() {
        return comments;
    }


    public void setComments(List<String> comments) {
        this.comments = comments;
    }


    public String getDescription() {
        return description;
    }


    public void setDescription(String description) {
        this.description = description;
    }


    public List<String> getKeywords() {
        return keywords;
    }


    public void setKeywords(List<String> keywords) {
        this.keywords = keywords;
    }


    public List<String> getLicenceUrl() {
        return licenceUrl;
    }


    public void setLicenceUrl(List<String> licenceUrl) {
        this.licenceUrl = licenceUrl;
    }


    public String getContentText() {
        return contentText;
    }


    public void setContentText(String contentText) {
        this.contentText = contentText;
    }


    public String getContentTextOriginalEncoding() {
        return contentTextOriginalEncoding;
    }


    public void setContentTextOriginalEncoding(String contentTextOriginalEncoding) {
        this.contentTextOriginalEncoding = contentTextOriginalEncoding;
    }


    public byte[] getContentFirstBytes() {
        return contentFirstBytes;
    }


    public void setContentFirstBytes(byte[] contentFirstBytes) {
        this.contentFirstBytes = contentFirstBytes;
    }


    public String getContentLanguage() {
        return contentLanguage;
    }


    public void setContentLanguage(String contentLanguage) {
        this.contentLanguage = contentLanguage;
    }


    public long getContentLength() {
        return contentLength;
    }


    public void setContentLength(long contentLength) {
        this.contentLength = contentLength;
    }


    public long getContentTextLength() {
        return contentTextLength;
    }


    public void setContentTextLength(long contentTextLength) {
        this.contentTextLength = contentTextLength;
    }


    public String getContentTypeDroid() {
        return contentTypeDroid;
    }


    public void setContentTypeDroid(String contentTypeDroid) {
        this.contentTypeDroid = contentTypeDroid;
    }


    public String getContentTypeExt() {
        return contentTypeExt;
    }


    public void setContentTypeExt(String contentTypeExt) {
        this.contentTypeExt = contentTypeExt;
    }


    public String getContentTypeFull() {
        return contentTypeFull;
    }


    public void setContentTypeFull(String contentTypeFull) {
        this.contentTypeFull = contentTypeFull;
    }


    public String getContentTypeNorm() {
        return contentTypeNorm;
    }


    public void setContentTypeNorm(String contentTypeNorm) {
        this.contentTypeNorm = contentTypeNorm;
    }


    public String getContentTypeServed() {
        return contentTypeServed;
    }


    public void setContentTypeServed(String contentTypeServed) {
        this.contentTypeServed = contentTypeServed;
    }


    public String getContentTypeTika() {
        return contentTypeTika;
    }


    public void setContentTypeTika(String contentTypeTika) {
        this.contentTypeTika = contentTypeTika;
    }


    public String getContentType() {
        return contentType;
    }


    public void setContentType(String contentType) {
        this.contentType = contentType;
    }


    public String getContentTypeVersion() {
        return contentTypeVersion;
    }


    public void setContentTypeVersion(String contentTypeVersion) {
        this.contentTypeVersion = contentTypeVersion;
    }


    public List<String> getElementsUsed() {
        return elementsUsed;
    }


    public void setElementsUsed(List<String> elementsUsed) {
        this.elementsUsed = elementsUsed;
    }


    public String getHash() {
        return hash;
    }


    public void setHash(String hash) {
        this.hash = hash;
    }


    public long getWaybackDate() {
        return waybackDate;
    }


    public void setWaybackDate(long waybackDate) {
        this.waybackDate = waybackDate;
    }


    public Date getCrawlDate() {
        return crawlDate;
    }


    public void setCrawlDate(Date crawlDate) {
        this.crawlDate = crawlDate;
    }


    public Date getLastModified() {
        return lastModified;
    }


    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }


    public String getUrlNorm() {
        return urlNorm;
    }


    public void setUrlNorm(String urlNorm) {
        this.urlNorm = urlNorm;
    }


    public String getUrlPath() {
        return urlPath;
    }


    public void setUrlPath(String urlPath) {
        this.urlPath = urlPath;
    }


    public String getUrl() {
        return url;
    }


    public void setUrl(String url) {
        this.url = url;
    }


    public String getUrlType() {
        return urlType;
    }


    public void setUrlType(String urlType) {
        this.urlType = urlType;
    }


    public String getDomain() {
        return domain;
    }


    public void setDomain(String domain) {
        this.domain = domain;
    }


    public String getHost() {
        return host;
    }


    public void setHost(String host) {
        this.host = host;
    }


    public List<String> getHostSurt() {
        return hostSurt;
    }


    public void setHostSurt(List<String> hostSurt) {
        this.hostSurt = hostSurt;
    }


    public String getPublicSuffix() {
        return publicSuffix;
    }


    public void setPublicSuffix(String publicSuffix) {
        this.publicSuffix = publicSuffix;
    }


    public String getResourcename() {
        return resourcename;
    }


    public void setResourcename(String resourcename) {
        this.resourcename = resourcename;
    }


    public String getResourcename_facet() {
        return resourcename_facet;
    }


    public void setResourcename_facet(String resourcename_facet) {
        this.resourcename_facet = resourcename_facet;
    }


    public List<String> getImageColours() {
        return imageColours;
    }


    public void setImageColours(List<String> imageColours) {
        this.imageColours = imageColours;
    }


    public String getImageDominantColour() {
        return imageDominantColour;
    }


    public void setImageDominantColour(String imageDominantColour) {
        this.imageDominantColour = imageDominantColour;
    }


    public int getImageFacesCount() {
        return imageFacesCount;
    }


    public void setImageFacesCount(int imageFacesCount) {
        this.imageFacesCount = imageFacesCount;
    }


    public List<String> getImageFaces() {
        return imageFaces;
    }


    public void setImageFaces(List<String> imageFaces) {
        this.imageFaces = imageFaces;
    }


    public long getImageHeight() {
        return imageHeight;
    }


    public void setImageHeight(long imageHeight) {
        this.imageHeight = imageHeight;
    }


    public long getImageWidth() {
        return imageWidth;
    }


    public void setImageWidth(long imageWidth) {
        this.imageWidth = imageWidth;
    }


    public long getImageSize() {
        return imageSize;
    }


    public void setImageSize(long imageSize) {
        this.imageSize = imageSize;
    }


    public List<String> getLinksImages() {
        return linksImages;
    }


    public void setLinksImages(List<String> linksImages) {
        this.linksImages = linksImages;
    }


    public List<String> getLinksDomains() {
        return linksDomains;
    }


    public void setLinksDomains(List<String> linksDomains) {
        this.linksDomains = linksDomains;
    }


    public List<String> getLinksHosts() {
        return linksHosts;
    }


    public void setLinksHosts(List<String> linksHosts) {
        this.linksHosts = linksHosts;
    }


    public List<String> getLinksHostsSurts() {
        return linksHostsSurts;
    }


    public void setLinksHostsSurts(List<String> linksHostsSurts) {
        this.linksHostsSurts = linksHostsSurts;
    }


    public List<String> getLinksPublicSuffixes() {
        return linksPublicSuffixes;
    }


    public void setLinksPublicSuffixes(List<String> linksPublicSuffixes) {
        this.linksPublicSuffixes = linksPublicSuffixes;
    }


    public List<String> getLinks() {
        return links;
    }


    public void setLinks(List<String> links) {
        this.links = links;
    }


    public List<String> getLocations() {
        return locations;
    }


    public void setLocations(List<String> locations) {
        this.locations = locations;
    }


    public List<String> getParseErrors() {
        return parseErrors;
    }


    public void setParseErrors(List<String> parseErrors) {
        this.parseErrors = parseErrors;
    }


    public List<String> getPdfPdfaErrors() {
        return pdfPdfaErrors;
    }


    public void setPdfPdfaErrors(List<String> pdfPdfaErrors) {
        this.pdfPdfaErrors = pdfPdfaErrors;
    }


    public String getPdfPdfaIsValid() {
        return pdfPdfaIsValid;
    }


    public void setPdfPdfaIsValid(String pdfPdfaIsValid) {
        this.pdfPdfaIsValid = pdfPdfaIsValid;
    }


    public List<String> getPostcodeDistrict() {
        return postcodeDistrict;
    }


    public void setPostcodeDistrict(List<String> postcodeDistrict) {
        this.postcodeDistrict = postcodeDistrict;
    }


    public List<String> getPostcode() {
        return postcode;
    }


    public void setPostcode(List<String> postcode) {
        this.postcode = postcode;
    }


    public Date getPublicationDate() {
        return publicationDate;
    }


    public void setPublicationDate(Date publicationDate) {
        this.publicationDate = publicationDate;
    }


    public int getPublicationYear() {
        return publicationYear;
    }


    public void setPublicationYear(int publicationYear) {
        this.publicationYear = publicationYear;
    }


    public String getRecordType() {
        return recordType;
    }


    public void setRecordType(String recordType) {
        this.recordType = recordType;
    }


    public List<String> getServer() {
        return server;
    }


    public void setServer(List<String> server) {
        this.server = server;
    }


    public int getStatusCode() {
        return statusCode;
    }


    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }


    public List<String> getGenerator() {
        return generator;
    }


    public void setGenerator(List<String> generator) {
        this.generator = generator;
    }


    public String getRedirectToNorm() {
        return redirectToNorm;
    }


    public void setRedirectToNorm(String redirectToNorm) {
        this.redirectToNorm = redirectToNorm;
    }


    public String getSourceFilePath() {
        return sourceFilePath;
    }


    public void setSourceFilePath(String sourceFilePath) {
        this.sourceFilePath = sourceFilePath;
    }


    public long getSourceFileOffset() {
        return sourceFileOffset;
    }


    public void setSourceFileOffset(long sourceFileOffset) {
        this.sourceFileOffset = sourceFileOffset;
    }


    public String getSourceFile() {
        return sourceFile;
    }


    public void setSourceFile(String sourceFile) {
        this.sourceFile = sourceFile;
    }

}