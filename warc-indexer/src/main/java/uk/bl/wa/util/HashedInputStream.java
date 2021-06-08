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

import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecordHeader;
import org.archive.util.Base32;
import org.jwat.common.RandomAccessFileInputStream;

import java.io.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.archive.format.warc.WARCConstants.HEADER_KEY_PAYLOAD_DIGEST;
import static org.archive.format.warc.WARCConstants.HEADER_KEY_TYPE;

/**
 * Utility method that takes a given input stream and wraps is in a SHA-1 {@link DigestInputStream}.
 */
public class HashedInputStream extends DigestInputStream {
    private static Logger log = LoggerFactory.getLogger(HashedInputStream.class );


    private final String url;
    private final boolean checkHash;

    private String headerHash = null;
    private String hash = null;
    private boolean hashMatched = false;
    private boolean isClosed = false;
    private boolean wasTruncated = false;
    private long bytesLeft;

    private static MessageDigest getDigester() {
        try {
            return MessageDigest.getInstance( MessageDigestAlgorithms.SHA_1);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error: Algorithm '" + MessageDigestAlgorithms.SHA_1 + "' not supported", e);
        }
    }

    /**
     * Wraps the in stream in a SHA-1 calculating DigestStream, guaranteeing that at most length bytes are read from
     * the in stream.
     * @param header (w)arc header which ideally contain the expected hash.
     * @param in a Stream with WARC record content.
     * @param length the length of the record.
     */
    public HashedInputStream(ArchiveRecordHeader header, InputStream in, long length ) {
        this(header.getUrl(),
             header.getHeaderFieldKeys().contains(HEADER_KEY_PAYLOAD_DIGEST) ?
                     Normalisation.sha1HashAsBase32((String) header.getHeaderValue(HEADER_KEY_PAYLOAD_DIGEST)) :
                     null,
             header.getHeaderFieldKeys().contains(HEADER_KEY_TYPE) &&
             header.getHeaderValue(HEADER_KEY_TYPE).equals(WARCConstants.WARCRecordType.response.toString()),
             in,
             length);
    }

    /**
     * Wraps the in stream in a SHA-1 calculating DigestStream, guaranteeing that at most length bytes are read from
     * the in stream.
     * @param url the URL for the content. Used for log messages.
     * @param expectedHash will be compared with the calculated hash.
     * @param checkHash if true, the expectedHash will be compared with the calculated, if false the expectedHash
     *                  will override the calculated.
     * @param in a Stream with WARC record content.
     * @param length the length of the record.
     */
    public HashedInputStream(String url, String expectedHash, boolean checkHash, InputStream in, long length ) {
        super(in, getDigester());
        bytesLeft = length;
        this.url = Normalisation.sanitiseWARCHeaderValue(url);
        this.headerHash = expectedHash;
        this.checkHash = checkHash;
    }

    @Override
    public int read() throws IOException {
        if (bytesLeft == 0) {
            return -1;
        }
        int b = super.read();
        if (b == -1) {
            bytesLeft = 0;
            wasTruncated = true;
        } else {
            bytesLeft--;
        }
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (bytesLeft == 0) {
            return -1;
        }
        if (len > bytesLeft) {
            len = (int) bytesLeft;
        }
        int readBytes = super.read(b, off, len);
        if (readBytes == -1) {
            bytesLeft = 0;
            wasTruncated = true;
        } else {
            bytesLeft -= readBytes;
        }
        return readBytes;
    }

    /**
     * Ensures that all bytes up to the previously given length has been read from the source stream and closes it.
     * Extracts the calculated hash and checks it against the header hash, writing an error to the log if there are
     * a mismatch.
     * @throws IOException if the source stream could not be closed.
     */
    @Override
    public void close() throws IOException {
        if (isClosed) {
            return;
        }
        if (bytesLeft > 0) {
            IOUtils.skip(this, bytesLeft);
        }
        isClosed = true;
        super.close();

        hash = "sha1:" + Base32.encode(digest.digest());
        // For response records, check the hash is consistent with any header hash:
        if ((headerHash == null) || (hash.length() != headerHash.length())) {
            return;
        }

        // We always perform a hash compare to make isHashMatched return the result, but we only complain
        // about problems if checkHash == true
        hashMatched = headerHash.equals(hash);

        if(checkHash) {
            if(!hashMatched) {
                log.warn(String.format(
                        "Hashes are not equal for '%s'. WARC-header: %s, content: %s", url, headerHash, hash));
            } else {
                log.debug("Hashes were found to match for '" + url + "'");
            }
        } else {
            // For revisit records, use the hash of the revisited payload:
            // TODO this should actually only do it for revisit type records.
            this.hash = this.headerHash;
        }
    }

    public boolean isClosed() {
        return isClosed;
    }

    /**
     * @return true if the stream containes less bytes than expected.
     */
    public boolean wasTruncated() {
        return wasTruncated;
    }

    /**
     * @return true if the header hash matches the calculated hash or checkHash == false.
     */
    public boolean isHashMatched() {
        if (!isClosed) {
            throw new IllegalStateException("Stream must be closed before calling isHashMatched()");
        }
        return hashMatched || !checkHash;
    }

    /**
     * @return the calculated hash.
     */
    public String getHash() {
        if (!isClosed) {
            throw new IllegalStateException("Stream must be closed before calling getHash()");
        }
        return hash;
    }

    /**
     * @return the header hash.
     */
    public String getHeaderHash() {
        return headerHash;
    }
}
