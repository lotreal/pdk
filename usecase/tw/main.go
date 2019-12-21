package tw

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

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
	SchemaFile       string
	FetchConcurrency int
	Concurrency      int
	Index            string
	BufferSize       int
	UseReadAll       bool

	indexer pdk.Indexer
	config  SchemaConfig
	urls    []string
	// bms     []pdk.ColumnMapper

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
	m.config = NewSchemaConfig(m.SchemaFile)

	err := m.readURLs()
	if err != nil {
		return err
	}

	gpSchema := NewPilosaSchema(m.Index, m.config)

	m.indexer, err = pdk.SetupPilosa([]string{m.PilosaHost}, m.Index, gpSchema, uint(m.BufferSize))
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

	// m.bms = GetBitMappers(m.SchemaFile)
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
			m.parseMapAndPost(records)
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
		return fmt.Errorf("need to specify a url file")
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

		scan := bufio.NewScanner(content)
		for scan.Scan() {
			m.totalRecs.Add(1)
			rec := scan.Text()
			m.addBytes(len(rec))
			// log.Printf("DM.rec: %s", rec)
			records <- CsvRecord{Val: rec, Type: '-'}
		}
		err := scan.Err()
		if err != nil {
			log.Printf("scan error on %s, err: %v", url, err)
		}
		delete(failedURLs, url)
	}
}

func (m *Main) parseMapAndPost(records <-chan CsvRecord) {
	for record := range records {
		InsertRecord(m.indexer, record, m.config)
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