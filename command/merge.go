package command

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/urfave/cli/v2"

	"github.com/peak/s5cmd/log/stat"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
)

var mergeHelpTemplate = `Name:
	{{.HelpName}} - {{.Usage}}

Usage:
	{{.HelpName}} [options] source

Options:
	{{range .VisibleFlags}}{{.}}
	{{end}}
Examples:
	1. Merge s3 objects into a file
		 > s5cmd {{.HelpName}} file-list.txt output.data
	2. 100 files will be downloaded at the same time
		 > s5cmd {{.HelpName}} -b 100 file-list.txt output.data
`

func NewMergeCommandFlags() []cli.Flag {
	mergeFlags := []cli.Flag{
		&cli.IntFlag{
			Name:    "batch-size",
			Aliases: []string{"b"},
			Value:   500,
			Usage:   "number of files will be downloaded at the same time. default is 500",
		},
	}
	return mergeFlags
}

func NewMergeCommand() *cli.Command {

	return &cli.Command{
		Name:               "merge",
		HelpName:           "merge",
		Usage:              "read the S3 objects and merge them into a single file. The given objects is provided in a text file",
		Flags:              NewMergeCommandFlags(),
		CustomHelpTemplate: mergeHelpTemplate,
		Before: func(c *cli.Context) error {
			err := validateMergeCommand(c)
			if err != nil {
				printError(commandFromContext(c), c.Command.Name, err)
			}
			return err
		},
		Action: func(c *cli.Context) (err error) {
			defer stat.Collect(c.Command.FullName(), &err)()

			src := c.Args().Get(0)
			dst := c.Args().Get(1)

			op := c.Command.Name
			fullCommand := commandFromContext(c)
			if err != nil {
				printError(fullCommand, op, err)
				return err
			}

			return Merge{
				src:         src,
				dst:         dst,
				op:          op,
				fullCommand: fullCommand,

				storageOpts: NewStorageOpts(c),
				batchSize:   c.Int("batch-size"),
			}.Run(c.Context)
		},
	}
}

// Merge holds merge operation flags and states.
type Merge struct {
	src         string
	dst         string
	op          string
	fullCommand string

	// s3 options
	storageOpts storage.Options
	batchSize   int
}

// Run prints content of given source to standard output.
func (m Merge) Run(ctx context.Context) error {
	// get file list
	objectKeys, err := readLines(m.src)
	checkError(err)

	objectKeyChunks := chunks(objectKeys, m.batchSize)
	outputFile, err := os.Create(m.dst)
	checkError(err)
	defer outputFile.Close()

	for chunkIdx, objectKeyChunk := range objectKeyChunks {
		logline := fmt.Sprintf("--- chunk idx: %d/%d", chunkIdx+1, len(objectKeyChunks))
		fmt.Println(logline)
		// wg := new(sync.WaitGroup)
		// wg.Add(len(objectKeyChunk))
		var objectDatas []chan []byte

		for _, objectKey := range objectKeyChunk {
			urlItems := splitAndTrim(objectKey, " --range ")
			srcurl, err := url.New(urlItems[0])
			checkError(err)

			if len(urlItems) == 2 {
				srcurl.Range = urlItems[1]
			}

			ch := make(chan []byte)
			objectDatas = append(objectDatas, ch)
			go m.doDownload(ctx, srcurl, ch)

		}
		// wg.Wait()

		for _, result := range objectDatas {
			_, err = outputFile.Write(<-result)
			checkError(err)
		}
		outputFile.Sync()
	}

	return nil
}

func validateMergeCommand(c *cli.Context) error {
	if c.Args().Len() != 2 {
		return fmt.Errorf("expected source and output arguments")
	}

	return nil
}

func (m Merge) doDownload(ctx context.Context, srcurl *url.URL, ch chan<- []byte) {
	// fmt.Println("doDownload", srcurl.Path)
	// defer wg.Done()
	client, err := storage.NewRemoteClient(ctx, srcurl, m.storageOpts)
	checkError(err)

	rc, err := client.Read(ctx, srcurl)
	checkError(err)
	defer rc.Close()

	body, err := ioutil.ReadAll(rc)
	checkError(err)

	ch <- body
}

func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	checkError(err)
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func chunks(xs []string, chunkSize int) [][]string {
	if len(xs) == 0 {
		return nil
	}
	divided := make([][]string, (len(xs)+chunkSize-1)/chunkSize)
	prev := 0
	i := 0
	till := len(xs) - chunkSize
	for prev < till {
		next := prev + chunkSize
		divided[i] = xs[prev:next]
		prev = next
		i++
	}
	divided[i] = xs[prev:]
	return divided
}

func splitAndTrim(input string, splitBy string) []string {
	slc := strings.Split(input, splitBy)
	for i := range slc {
		slc[i] = strings.TrimSpace(slc[i])
	}
	return slc
}

func checkError(e error) {
	if e != nil {
		panic(e)
	}
}
