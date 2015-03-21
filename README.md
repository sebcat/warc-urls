# warc-urls
--
Reads WARC-Target-URIs from from WARC headers and outputs them to standard
output. Concurrent WARC record processing. Testbed for github.com/sebcat/warc.

Example:

    $ ./warc-urls -warc ../warc/testdata/lel.warc.gz >> urls.txt
    2015/03/21 07:13:29 processed 579 records in 863.826297ms
