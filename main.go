// Reads WARC-Target-URIs from from WARC headers and outputs them to
// standard output. Concurrent WARC record processing. Testbed for
// github.com/sebcat/warc.
//
// Example:
//     $ ./warc-urls -warc ../warc/testdata/lel.warc.gz >> urls.txt
//     2015/03/21 07:13:29 processed 579 records in 863.826297ms
package main

import (
	"flag"
	"github.com/sebcat/warc"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"time"
)

var (
	warcFile    = flag.String("warc", "", "path to WARC file")
	nconcurrent = flag.Int("n-concurrent", 4, "number of concurrent WARCers")
	cpuprofile  = flag.String("cpuprofile", "", "write CPU profile to file")
)

func readRecords(path string, recs chan []byte, nrecords *int) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()
	r, err := warc.NewGZIPReader(f)
	if err != nil {
		log.Fatal(err)
	}

	for {
		rec, err := r.NextRaw()
		if err == io.EOF {
			break
		} else if err == warc.ErrMalformedRecord {
			log.Println("readWARCRecords", err)
		} else if err != nil {
			log.Fatal("readWARCRecords", err)
		}

		recs <- rec
		if nrecords != nil {
			*nrecords++
		}
	}

	close(recs)
}

func record(recs chan []byte, urls chan string) {
	for rec := range recs {
		var r warc.Record
		if err := r.FromBytes(rec); err != nil {
			log.Println("processRecords", err)
			continue
		}

		target := r.Fields.Value("WARC-Target-URI")
		target = strings.Trim(target, " \t")
		if len(target) > 0 {
			target += "\n"
			urls <- target
		}
	}
}

func processRecords(recs chan []byte, urls chan string, nconcurrent int) {
	var wg sync.WaitGroup

	wg.Add(nconcurrent)
	for i := 0; i < nconcurrent; i++ {
		go func() {
			record(recs, urls)
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(urls)
	}()
}

func writeURLs(urls chan string, done chan struct{}) {
	// might grow large, maybe use hashes instead
	// or if you're into *large* stuff, use the disk
	existing := make(map[string]struct{})

	for url := range urls {
		if _, exists := existing[url]; !exists {
			var x struct{}
			existing[url] = x
			os.Stdout.WriteString(url)
		}
	}

	close(done)
}

func main() {
	flag.Parse()

	if len(*warcFile) == 0 {
		log.Fatal("-warc not set")
	}

	if *nconcurrent <= 0 {
		log.Fatal("invalid -n-concurrent setting")
	}

	if len(*cpuprofile) > 0 {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}

		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	var nrecords int
	recChan := make(chan []byte)
	urlChan := make(chan string)
	doneChan := make(chan struct{}, 1)

	go readRecords(*warcFile, recChan, &nrecords)
	go processRecords(recChan, urlChan, *nconcurrent)
	go writeURLs(urlChan, doneChan)

	started := time.Now()
	<-doneChan
	log.Printf("processed %v records in %v\n", nrecords, time.Since(started))
}
