/**
 * An attempt to speed up language detection in the Tika code base.
 * The relevant part is {@link uk.bl.wa.analyser.text.lang.LanguageProfile#distance(LanguageProfile)}
 * where the old implementation used temporary sets of Strings, while the new implementation uses
 * sorted lists of map entries. In O-notation, the runtimes are the same. Empirically, local tests
 * shows that the speed of text detection is a bit more than doubled.
 * </p><p>
 * The changes have been upstreamed Tika in the yet-to-be-released 1.8.
 * The warc-indexer classes in this package should be removed when switching to Tika 1.8.
 * </p><p>
 * Future improvements: The creation of LanguageProfiles could match the packed representation
 * used by the Interleaver, which should reduce the number of Object-allocations and speed up
 * creation of the cached char[].
 */
package uk.bl.wa.analyser.text.lang;

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
