package genome

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"net/http"
	_ "net/http/pprof"

	"github.com/pilosa/pdk"
	"github.com/pkg/errors"
)

const SLICEWIDTH = 8388608
const BASECOUNT = 4

// SubjecterOpts are options for the Subjecter.
type SubjecterOpts struct {
	Path []string `help:"Path to subject."`
}

// Chromosome holds FASTA data in a string for a particular chromosome.
type Chromosome struct {
	data   string
	number int
	offset uint64
}

// Main holds the config for the http command.
type Main struct {
	File        string   `help:"Path to FASTA file."`
	Hosts       []string `help:"Pilosa hosts."`
	Index       string   `help:"Pilosa index."`
	Min         int      `help:"Minimum number of random mutations per [denom]."`
	Max         int      `help:"Maximum number of random mutations per [denom]."`
	Denom       int      `help:"Denominator to use for calculating random mutations."`
	Count       uint64   `help:"Number of mutated rows to create."`
	Concurrency int      `help:"Number of slice importers to run simultaneously."`

	index pdk.Indexer

	chromosomes []*Chromosome
}

// NewMain gets a new Main with default values.
func NewMain() *Main {
	return &Main{
		Hosts:       []string{":10101"},
		Index:       "genome",
		Min:         10,
		Max:         50,
		Denom:       10000,
		Count:       10,
		Concurrency: 8,
	}
}

var frame = "sequences"

var frames = []pdk.FrameSpec{
	pdk.NewRankedFrameSpec(frame, 0),
}

// Run runs the genome command.
func (m *Main) Run() error {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	var err error

	// Load FASTA file.
	start := time.Now()
	log.Printf("Start file load at %v", start)
	err = m.loadFile(m.File)
	if err != nil {
		return errors.Wrap(err, "loading file")
	}
	log.Printf("Done file load at %v", time.Since(start))

	m.index, err = pdk.SetupPilosa(m.Hosts, m.Index, frames, 1000000)
	if err != nil {
		return errors.Wrap(err, "setting up Pilosa")
	}

	// Mutator setup.
	rand.Seed(time.Now().UTC().UnixNano())

	nopmut, err := NewMutator(0, 1, 1)
	if err != nil {
		return errors.Wrap(err, "making no-op mutator")
	}
	mut, err := NewMutator(m.Min, m.Max, m.Denom)
	if err != nil {
		return errors.Wrap(err, "making mutator")
	}

	for row := uint64(0); row < m.Count; row++ {
		start = time.Now()
		log.Printf("Start row %d at %v", row, start)
		mutator := mut
		if row == 0 {
			mutator = nopmut
		} else {
			mutator.setRandomMatch()
		}
		sliceChan := make(chan uint64, 1000)
		eg := &errgroup.Group{}
		for i := 0; i < m.Concurrency; i++ {
			eg.Go(func() error {
				return m.importSlices(row, sliceChan, mutator)
			})
		}
		for s := uint64(0); s < m.maxSlice(); s++ {
			sliceChan <- s
		}
		close(sliceChan)
		err = eg.Wait()
		if err != nil {
			return errors.Wrapf(err, "importing row %d", row)
		}
		log.Printf("Done row %d in %v", row, time.Since(start))
	}

	return nil
}

func (m *Main) maxSlice() uint64 {
	lastChrom := m.chromosomes[len(m.chromosomes)-1]
	maxCol := lastChrom.offset + uint64(len(lastChrom.data)) - 1
	return BASECOUNT * maxCol / SLICEWIDTH
}

// loadFile loads the data from file into m.chromosomes.
func (m *Main) loadFile(f string) error {
	file, err := os.Open(f)
	if err != nil {
		return errors.Wrap(err, "opening FASTA file")
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	crNumber := 0
	colCount := uint64(0)

	var chr *Chromosome
	var builder strings.Builder

	for scanner.Scan() {
		line := scanner.Text()

		// HEADER
		if strings.HasPrefix(line, ">") {
			if chr != nil {
				chr.data = builder.String()
			}
			parts := strings.Split(line, " ")
			name := parts[0][1:]
			if !strings.Contains(name, "chr") {
				log.Printf("end of useful info (%v)\n", line)
				break
			}
			crID := name[3:]
			if crID == "X" {
				crNumber = 23
			} else if crID == "Y" {
				crNumber = 24
			} else if crID == "M" {
				crNumber = 25
			} else {
				crNumber, err = strconv.Atoi(crID)
				if err != nil {
					return err
				}
			}
			fmt.Printf("'%v' %v %v %v\n", line, name, crID, crNumber)

			chr = &Chromosome{
				number: crNumber,
				offset: colCount,
			}
			m.chromosomes = append(m.chromosomes, chr)
			builder = strings.Builder{}

			continue
		}

		// LINE
		builder.WriteString(line)
		colCount += uint64(len(line))
	}

	chr.data = builder.String()

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return nil
}

var errSentinel = errors.New("SENTINEL")

type rangeSlice [][]uint64

func (r rangeSlice) Len() int      { return len(r) }
func (r rangeSlice) Swap(i, j int) { r[i], r[j] = r[j], r[i] }

// Compare ranges by first key of the sub-slice
func (r rangeSlice) Less(i, j int) bool { return r[i][0] < r[j][0] }

func (m *Main) getGenomeSlice(slice uint64) string {

	sw := uint64(SLICEWIDTH / BASECOUNT)

	// make a slice of chromosome position ranges
	var r [][]uint64
	for i, chr := range m.chromosomes {
		r = append(r, []uint64{chr.offset, chr.offset + uint64(len(chr.data)) - 1, uint64(i)})
	}
	sort.Sort(rangeSlice(r))

	posStart := slice * sw
	posEnd := posStart + sw - 1

	var s string
	var chrCount int
	for _, rng := range r {
		if len(s) > 0 {
			if posEnd <= rng[1] {
				return s + m.chromosomes[rng[2]].data[:posEnd-rng[0]+1]
			}
			s += m.chromosomes[rng[2]].data
			chrCount++
			continue
		}
		if posStart >= rng[0] && posStart <= rng[1] {
			// slice is contained in a single chromosome
			if posEnd <= rng[1] {
				return m.chromosomes[rng[2]].data[posStart-rng[0] : posEnd-rng[0]+1]
			}
			if len(s) == 0 {
				s = m.chromosomes[rng[2]].data[posStart-rng[0]:]
				chrCount++
				continue
			}
		}
	}
	return s
}

func (m *Main) importSlices(row uint64, sliceChan chan uint64, mutator *Mutator) error {
	client := m.index.Client()

	for slice := range sliceChan {
		startCol := slice * SLICEWIDTH
		bases := m.getGenomeSlice(slice)
		rows := make([]uint64, 0, 1048576)
		cols := make([]uint64, 0, 1048576)
		for i, letter := range bases {
			chr := mutator.mutate(string(letter))
			colz := fastaCodeToNucleotides(chr)
			for _, col := range colz {
				rows = append(rows, row)
				cols = append(cols, startCol+uint64(i)*BASECOUNT+uint64(col))
			}
		}
		err := errSentinel
		tries := 0
		for err != nil {
			tries++
			if tries > 10 {
				return err
			}
			err = client.SliceImport(m.Index, frame, slice, rows, cols)
			if err != nil {
				log.Printf("Error importing slice %d, retrying: %v", slice, err)
			}
		}
	}
	return nil
}
