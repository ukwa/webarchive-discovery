#!/bin/bash
#
# This script uses the Regular Expression Indexer to find postcodes in WARC record payloads.
#
# This does not parse the content using Tika.
#

if [ $# -ne 2 ]; then
    echo USAGE: generate-postcode-index.sh [file-listing-input-files] [output-folder]
    exit 1
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

hadoop jar $DIR/../target/warc-hadoop-recordreaders-3.0.0-SNAPSHOT.jar uk.bl.wap.hadoop.regex.WARCRegexIndexer $1 $2 "[A-Z]{1,2}[0-9R][0-9A-Z]? [0-9][ABD-HJLNP-UW-Z]{2}"
