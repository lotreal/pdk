package cmd

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/pilosa/pdk/usecase/tw"
	"github.com/spf13/cobra"
)

// TwMain is wrapped by NewTwCommand. It is exported for testing purposes.
var TwMain *tw.Main

// NewTwCommand wraps tw.Main with cobra.Command for use from a CLI.
func NewTwCommand(stdin io.Reader, stdout, stderr io.Writer) *cobra.Command {
	TwMain = tw.NewMain()
	twCommand := &cobra.Command{
		Use:   "tw",
		Short: "Import tw data to Pilosa.",
		RunE: func(cmd *cobra.Command, args []string) error {
			start := time.Now()
			err := TwMain.Run()
			if err != nil {
				return err
			}
			dt := time.Since(start)
			log.Println("Done: ", dt)
			fmt.Printf("{\"duration\": %f}\n", dt.Seconds())
			return nil
		},
	}
	flags := twCommand.Flags()
	flags.IntVarP(&TwMain.Concurrency, "concurrency", "c", 8, "Number of goroutines fetching and parsing")
	flags.IntVarP(&TwMain.FetchConcurrency, "fetch-concurrency", "e", 8, "Number of goroutines fetching and parsing")
	flags.IntVarP(&TwMain.BufferSize, "buffer-size", "b", 1000000, "Size of buffer for importers - heavily affects memory usage")
	flags.BoolVarP(&TwMain.UseReadAll, "use-read-all", "", false, "Setting to true uses much more memory, but ensures that an entire file can be read before beginning to parse it.")
	flags.StringVarP(&TwMain.PilosaHost, "pilosa", "p", "localhost:10101", "Pilosa host")
	flags.StringVarP(&TwMain.Index, "index", "i", TwMain.Index, "Pilosa db to write to")
	flags.StringVarP(&TwMain.URLFile, "url-file", "f", "usecase/tw/urls-short.txt", "File to get raw data urls from. Urls may be http or local files.")

	return twCommand
}

func init() {
	subcommandFns["tw"] = NewTwCommand
}
