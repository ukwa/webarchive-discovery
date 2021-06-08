/**
 * 
 */
package uk.bl.wa.util;

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

import static org.archive.format.warc.WARCConstants.HEADER_KEY_PAYLOAD_DIGEST;
import static org.archive.format.warc.WARCConstants.HEADER_KEY_TYPE;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecordHeader;
import org.archive.util.Base32;
import org.jwat.common.RandomAccessFileInputStream;

/**
 * Utility method that takes a given input stream and caches the
 * content in RAM, on disk, based on some size limits.
 * 
 * Also calculates the hash of the whole stream.
 * 
 * @author anj
 *
 */
public class HashedCachedInputStream {
    private static Logger log = LoggerFactory.getLogger(HashedCachedInputStream.class );
    
    private MessageDigest digest = null;
    
    private String headerHash = null;
    
    private String hash = null;

    private boolean hashMatched = false;

    private boolean inMemory;
    
    private File cacheFile;
    private RandomAccessFile RAFcache;
    private String url;

    private byte[] cacheBytes;
    
    private boolean truncated = false;
    
    // Thresholds:
    private long inMemoryThreshold = 1024*1024; // Up to 1MB allowed in RAM.
    private long onDiskThreshold = 1024*1024*100; // Up to 100MB cached on disk. 
    
    /**
     * 
     * @param header
     * @param in
     * @param length
     * @param inMemoryThreshold
     * @param onDiskThreshold
     */
    public HashedCachedInputStream( ArchiveRecordHeader header, InputStream in, long length, long inMemoryThreshold, long onDiskThreshold ) {
        this.inMemoryThreshold = inMemoryThreshold;
        this.onDiskThreshold = onDiskThreshold;
        init(header,in,length);
    }

    /**
     * Constructo, processed payload for hash and makes content available.
     * 
     * @param header
     * @param in
     * @param length
     */
    public HashedCachedInputStream( ArchiveRecordHeader header, InputStream in, long length ) {
        init(header,in,length);
    }
    
    /**
     * @param header
     * @param in
     * @param length
     */
    private void init(ArchiveRecordHeader header, InputStream in, long length) {
        url = Normalisation.sanitiseWARCHeaderValue(header.getUrl());
        try {
            digest =  MessageDigest.getInstance( MessageDigestAlgorithms.SHA_1);
        } catch (NoSuchAlgorithmException e) {
            log.error( "Hashing: " + url + "@" + header.getOffset(), e );
        }
        
        try {
            if( header.getHeaderFieldKeys().contains( HEADER_KEY_PAYLOAD_DIGEST ) ) {
                headerHash = Normalisation.sha1HashAsBase32((String) header.getHeaderValue(HEADER_KEY_PAYLOAD_DIGEST));
            }
            
            // Create a suitable outputstream for caching the content:
            OutputStream cache = null;
            if( length < inMemoryThreshold ) {
                inMemory = true;
                cache = new ByteArrayOutputStream();
            } else {
                inMemory = false;
                cacheFile = File.createTempFile("warc-indexer", ".cache");
                cacheFile.deleteOnExit();
                cache = new FileOutputStream( cacheFile );
            }
                
            DigestInputStream dinput = new DigestInputStream( in, digest );
            
            long toCopy = length;
            if( length > this.onDiskThreshold ) {
                toCopy = this.onDiskThreshold;
            }
            IOUtils.copyLarge( dinput, cache, 0, toCopy);
            cache.close();

            // Read the remainder of the stream, to get the hash.
            if( length > this.onDiskThreshold ) {
                truncated = true;
                // IOUtils.skip( dinput, length - this.onDiskThreshold);
                IOUtils.toString(dinput);
            }
            
            // Now set up the inputStream
            if (inMemory) {
                this.cacheBytes = ((ByteArrayOutputStream) cache).toByteArray();
                // Encourage GC
                cache = null;
            }
            
            hash = "sha1:" + Base32.encode(digest.digest());

            // For response records, check the hash is consistent with any header hash:
            if( (headerHash != null) && (hash.length() == headerHash.length())) {
                if( header.getHeaderFieldKeys().contains( HEADER_KEY_TYPE ) &&
                    header.getHeaderValue( HEADER_KEY_TYPE ).equals(WARCConstants.WARCRecordType.response.toString())
                        ) {
                    if( ! headerHash.equals(hash)) {
                        log.error("Hashes are not equal for this input!");
                        log.error(
                                " - payload hash from header = " + headerHash);
                        log.error(" - payload hash from content = " + hash);
                        this.hashMatched = false;
                    } else {
                        this.hashMatched = true;
                        log.debug("Hashes were found to match for " + url);
                    }
                } else {
                    // For revisit records, use the hash of the revisited payload:
                    // TODO this should actually only do it for revisit type records.
                    this.hash = this.headerHash;
                }
            }
            
        } catch( Exception i ) {
            log.error( "Hashing: " + url + "@" + header.getOffset(), i );
        }        
    }
    
    /**
     * @return the hashMatched
     */
    public boolean isHashMatched() {
        return hashMatched;
    }

    /**
     * 
     * @return
     */
    public String getHash() {
        return hash;
    }
    
    /**
     * @return the headerHash
     */
    public String getHeaderHash() {
        return headerHash;
    }

    /**
     * This returns the content. {@link #cleanup()} should be called after use
     * as this avoids a build-up of temporary files.
     * 
     * @return a {@link InputStream#mark(int)}-capable InputStream with the
     *         content given in the constructor.
     */
    public InputStream getInputStream() {
        if( inMemory ) {
            if( this.cacheBytes != null ) {
                return new ByteArrayInputStream( this.cacheBytes );
            } else {
                log.error("Found a NULL byte array!");
                return new ByteArrayInputStream( new byte[] {} );
            }
        } else {
            try {
                RAFcache = new RandomAccessFile(cacheFile, "r");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return null;
            }
            return new RandomAccessFileInputStream(RAFcache);
        }
    }
    
    /**
     * 
     * @return
     */
    public boolean isTruncated() {
        return truncated;
    }
    
    /**
     * Closes all references to the cache file (if any) and deletes the file.
     * 
     * Failure to call this method after use of the content will lead to a build-up of temporary files for as
     * long as the JVM is running.
     */
    public void cleanup() {
        if (RAFcache != null) {
            try {
                RAFcache.close();
            } catch (Exception e) {
                log.warn("Exception closing RandomAccessFile cache for " + cacheFile + " for " + url, e);
            }
        }

        if( this.cacheFile != null && cacheFile.exists() ) {
            try {
                if (!this.cacheFile.delete()) {
                    log.warn("Unable to delete " + cacheFile + " for " + url);
                }
            } catch (Exception e) {
                log.warn("Exception deleting " + cacheFile + " for " + url, e);
            }
        }
    }

}
