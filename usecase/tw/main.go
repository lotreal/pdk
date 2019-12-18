package tw

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	// for profiling
	_ "net/http/pprof"

	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

const (
	INDEX = "dm"
)

// Main holds options and execution state for usecase.
type Main struct {
	PilosaHost       string
	URLFile          string
	FetchConcurrency int
	Concurrency      int
	Index            string
	BufferSize       int
	UseReadAll       bool

	indexer pdk.Indexer
	urls    []string
	bms     []pdk.ColumnMapper

	nexter pdk.INexter

	totalBytes int64
	bytesLock  sync.Mutex

	totalRecs     *counter
	skippedRecs   *counter
	nullLocs      *counter
	badLocs       *counter
	badSpeeds     *counter
	badTotalAmnts *counter
	badDurations  *counter
	badPassCounts *counter
	badDist       *counter
	badUnknowns   *counter
}

// NewMain returns a new instance of Main with default values.
func NewMain() *Main {
	m := &Main{
		Concurrency:      1,
		FetchConcurrency: 1,
		Index:            INDEX,
		nexter:           pdk.NewNexter(),
		urls:             make([]string, 0),

		totalRecs:     &counter{},
		skippedRecs:   &counter{},
		nullLocs:      &counter{},
		badLocs:       &counter{},
		badSpeeds:     &counter{},
		badTotalAmnts: &counter{},
		badDurations:  &counter{},
		badPassCounts: &counter{},
		badDist:       &counter{},
		badUnknowns:   &counter{},
	}

	return m
}

// Run runs the taxi usecase.
func (m *Main) Run() error {
	go func() {
		// ?
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	err := m.readURLs()
	if err != nil {
		return err
	}

	schema := CreateSchema(m.Index)

	m.indexer, err = pdk.SetupPilosa([]string{m.PilosaHost}, m.Index, schema, uint(m.BufferSize))
	if err != nil {
		return errors.Wrap(err, "setting up indexer")
	}

	ticker := m.printStats()

	urls := make(chan string, 100)
	records := make(chan CsvRecord, 10000)

	go func() {
		for _, url := range m.urls {
			urls <- url
		}
		close(urls)
	}()

	m.bms = GetBitMappers()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			log.Printf("Rides: %d, Bytes: %s", m.nexter.Last(), pdk.Bytes(m.bytesProcessed()))
			os.Exit(0)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < m.FetchConcurrency; i++ {
		wg.Add(1)
		go func() {
			m.fetch(urls, records)
			wg.Done()
		}()
	}
	var wg2 sync.WaitGroup
	for i := 0; i < m.Concurrency; i++ {
		wg2.Add(1)
		go func(i int) {
			m.parseMapAndPost(records, i)
			wg2.Done()
		}(i)
	}
	wg.Wait()
	close(records)
	wg2.Wait()
	err = m.indexer.Close()
	ticker.Stop()
	// print stats one last time
	bytes := m.bytesProcessed()

	log.Printf("Rides: %d, Bytes: %s, Records: %v", m.nexter.Last(), pdk.Bytes(bytes), m.totalRecs.Get())
	log.Printf("Skipped: %v, badLocs: %v, nullLocs: %v, badSpeeds: %v, badTotalAmnts: %v, badDurations: %v, badUnknowns: %v, badPassCounts: %v, badDist: %v", m.skippedRecs.Get(), m.badLocs.Get(), m.nullLocs.Get(), m.badSpeeds.Get(), m.badTotalAmnts.Get(), m.badDurations.Get(), m.badUnknowns.Get(), m.badPassCounts.Get(), m.badDist.Get())
	return errors.Wrap(err, "closing indexer")
}

func (m *Main) readURLs() error {
	if m.URLFile == "" {
		return fmt.Errorf("Need to specify a URL File")
	}
	f, err := os.Open(m.URLFile)
	if err != nil {
		return errors.Wrap(err, "opening url file")
	}
	s := bufio.NewScanner(f)
	for s.Scan() {
		m.urls = append(m.urls, s.Text())
	}
	err = s.Err()
	return errors.Wrap(err, "scanning url file")
}

func (m *Main) printStats() *time.Ticker {
	t := time.NewTicker(time.Second * 10)
	start := time.Now()
	go func() {
		for range t.C {
			duration := time.Since(start)
			bytes := m.bytesProcessed()
			log.Printf("Rides: %d, Bytes: %s, Records: %v, Duration: %v, Rate: %v/s", m.nexter.Last(), pdk.Bytes(bytes), m.totalRecs.Get(), duration, pdk.Bytes(float64(bytes)/duration.Seconds()))
			log.Printf("Skipped: %v, badLocs: %v, nullLocs: %v, badSpeeds: %v, badTotalAmnts: %v, badDurations: %v, badUnknowns: %v, badPassCounts: %v, badDist: %v", m.skippedRecs.Get(), m.badLocs.Get(), m.nullLocs.Get(), m.badSpeeds.Get(), m.badTotalAmnts.Get(), m.badDurations.Get(), m.badUnknowns.Get(), m.badPassCounts.Get(), m.badDist.Get())
		}
	}()
	return t
}

// getNextURL fetches the next url from the channel, or if it is emtpy, gets a
// url from the failedURLs map after 10 seconds of waiting on the channel. As
// long as it gets a url, its boolean return value is true - if it does not get
// a url, it returns false.
func getNextURL(urls <-chan string, failedURLs map[string]int) (string, bool) {
	url, open := <-urls
	if !open {
		for url := range failedURLs {
			return url, true
		}
		return "", false
	}
	return url, true
}

func (m *Main) fetch(urls <-chan string, records chan<- CsvRecord) {
	failedURLs := make(map[string]int)
	for {
		url, ok := getNextURL(urls, failedURLs)
		if !ok {
			break
		}

		var content io.ReadCloser
		if strings.HasPrefix(url, "http") {
			resp, err := http.Get(url)
			if err != nil {
				log.Printf("fetching %s, err: %v", url, err)
				continue
			}
			content = resp.Body
		} else {
			f, err := os.Open(url)
			if err != nil {
				log.Printf("opening %s, err: %v", url, err)
				continue
			}
			content = f
		}
		var scan *bufio.Scanner
		if m.UseReadAll {
			// we're using ReadAll here to ensure that we can read the entire
			// file/url before we start putting it into Pilosa. Not great for memory
			// usage or smooth performance, but we want to ensure repeatable results
			// in the simplest way possible.
			contentBytes, err := ioutil.ReadAll(content)
			if err != nil {
				failedURLs[url]++
				if failedURLs[url] > 10 {
					log.Fatalf("Unrecoverable failure while fetching url: %v, err: %v. Could not read fully after 10 tries.", url, err)
				}
				continue
			}
			err = content.Close()
			if err != nil {
				log.Printf("closing %s, err: %v", url, err)
			}

			buf := bytes.NewBuffer(contentBytes)
			scan = bufio.NewScanner(buf)
		} else {
			scan = bufio.NewScanner(content)
		}

		for scan.Scan() {
			m.totalRecs.Add(1)
			rec := scan.Text()
			m.addBytes(len(rec))
			log.Printf("DM.rec: %s", rec)
			records <- CsvRecord{Val: rec, Type: '-'}
		}
		err := scan.Err()
		if err != nil {
			log.Printf("scan error on %s, err: %v", url, err)
		}
		delete(failedURLs, url)
	}
}

type CsvRecord struct {
	Type rune
	Val  string
}

func (r CsvRecord) clean() ([]string, bool) {
	if len(r.Val) == 0 {
		return nil, false
	}
	fields := strings.Split(r.Val, ",")
	return fields, true
}

type columnField struct {
	Column uint64
	Field  string
}

type valField struct {
	Val   int64
	Frame string
	Field string
}

func (m *Main) parseMapAndPost(records <-chan CsvRecord, num int) {
Records:
	for record := range records {
		fields, ok := record.clean()
		if !ok {
			m.skippedRecs.Add(1)
			continue
		}

		log.Printf("DM.fields: %s", fields)

		valsToSet := make([]valField, 0)
		columnsToSet := make([]columnField, 0)

		for _, bm := range m.bms {
			if len(bm.Fields) != len(bm.Parsers) {
				// TODO if len(pm.Parsers) == 1, use that for all fields
				log.Fatalf("parse: BitMapper has different number of fields: %v and parsers: %v", bm.Fields, bm.Parsers)
			}

			// parse fields into a slice `parsed`
			parsed := make([]interface{}, 0, len(bm.Fields))
			for n, fieldnum := range bm.Fields {
				parser := bm.Parsers[n]
				if fieldnum >= len(fields) {
					log.Printf("parse: field index: %v out of range for: %v", fieldnum, fields)
					m.skippedRecs.Add(1)
					continue Records
				}
				parsedField, err := parser.Parse(fields[fieldnum])
				if err != nil && fields[fieldnum] == "" {
					m.skippedRecs.Add(1)
					continue Records
				} else if err != nil {
					log.Printf("parsing: field: %v err: %v bm: %v rec: %v", fields[fieldnum], err, bm, record)
					m.skippedRecs.Add(1)
					continue Records
				}
				parsed = append(parsed, parsedField)
			}

			// map those fields to a slice of IDs
			ids, err := bm.Mapper.ID(parsed...)
			if err != nil {
				if err.Error() == "point (0, 0) out of range" {
					m.nullLocs.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				if strings.Contains(bm.Field, "grid_id") && strings.Contains(err.Error(), "out of range") {
					m.badLocs.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				if bm.Field == "speed_mph" && strings.Contains(err.Error(), "out of range") {
					m.badSpeeds.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				if bm.Field == "total_amount_dollars" && strings.Contains(err.Error(), "out of range") {
					m.badTotalAmnts.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				if bm.Field == "duration_minutes" && strings.Contains(err.Error(), "out of range") {
					m.badDurations.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				if bm.Field == "passenger_count" && strings.Contains(err.Error(), "out of range") {
					m.badPassCounts.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				if bm.Field == "dist_miles" && strings.Contains(err.Error(), "out of range") {
					m.badDist.Add(1)
					m.skippedRecs.Add(1)
					continue Records
				}
				log.Printf("mapping: bm: %v, err: %v rec: %v", bm, err, record)
				m.skippedRecs.Add(1)
				m.badUnknowns.Add(1)
				continue Records
			}
			// begin quick hack to extract cost for setting in a BSI field.
			if bm.Field == "total_amount_dollars" {
				cents := parsed[0].(float64) * 100.0
				if cents > 0 { // guard against bad data.
					valsToSet = append(valsToSet, valField{Val: int64(cents), Frame: "cost_cents", Field: "cost_cents"})
				}
			}
			// end quick cost hack

			for _, id := range ids {
				columnsToSet = append(columnsToSet, columnField{Column: uint64(id), Field: bm.Field})
			}
		}
		columnsToSet = append(columnsToSet)
		columnID := m.nexter.Next()
		for _, bit := range columnsToSet {
			m.indexer.AddColumn(bit.Field, columnID, bit.Column)
		}
		for _, val := range valsToSet {
			m.indexer.AddValue(val.Field, columnID, val.Val)
		}
	}
}

func (m *Main) addBytes(n int) {
	m.bytesLock.Lock()
	m.totalBytes += int64(n)
	m.bytesLock.Unlock()
}

func (m *Main) bytesProcessed() (num int64) {
	m.bytesLock.Lock()
	num = m.totalBytes
	m.bytesLock.Unlock()
	return
}

type counter struct {
	num  int64
	lock sync.Mutex
}

func (c *counter) Add(n int) {
	c.lock.Lock()
	c.num += int64(n)
	c.lock.Unlock()
}

func (c *counter) Get() (ret int64) {
	c.lock.Lock()
	ret = c.num
	c.lock.Unlock()
	return
}

func MappingRecord(record CsvRecord) error {
	fields, ok := record.clean()
	if !ok {
		return fmt.Errorf("record %s not valid", record)
	}

	log.Printf("DM.fields: %s", fields)
	return nil
}
