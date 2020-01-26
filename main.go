package main

import (
	"crypto/md5"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/akamensky/argparse"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	parser := argparse.NewParser("godiskload", "Disk load emulation")
	path := parser.String(
		"p", "path",
		&argparse.Options{
			Help:    "Path",
			Default: "testing",
		},
	)
	writeAlg := parser.Selector(
		"w", "write",
		[]string{"line", "random"},
		&argparse.Options{
			Help:    "Load algorithm (line, random)",
			Default: "line",
		},
	)
	iterations := parser.Int(
		"i", "iterations",
		&argparse.Options{
			Help:    "Count of thousands of operations",
			Default: 5,
		},
	)
	compact := parser.Flag(
		"c", "compact",
		&argparse.Options{
			Help:    "Compact in every thousand ops",
			Default: false,
		},
	)
	tableSize := parser.Int(
		"t", "table-size",
		&argparse.Options{
			Help:    "Table size in KB",
			Default: 2 * 1024,
		},
	)
	fsync := parser.Flag(
		"s", "fsync",
		&argparse.Options{
			Help:    "Run fsync on write",
			Default: false,
		},
	)

	err := parser.Parse(os.Args)
	if err != nil {
		fmt.Print(parser.Usage(err))
		return
	}

	db, err := leveldb.OpenFile(*path, &opt.Options{
		CompactionTableSize:    (*tableSize * 1024),
		WriteBuffer:            (*tableSize * 1024) * 4,
		Compression:            opt.NoCompression,
		DisableBlockCache:      true,
		OpenFilesCacheCapacity: -1,
	})
	if err != nil {
		log.Panic(err)
	}

	defer db.Close()

	log.Println("Start")

	for i := 0; i < *iterations; i++ {
		for y := 0; y < 1000; y++ {
			key, value := genData(*writeAlg)
			db.Put(key, value, &opt.WriteOptions{
				Sync: *fsync,
			})
		}

		if *compact {
			log.Println("Start Compact", i+1)
			db.CompactRange(util.Range{})
			log.Println("End Compact")
		}
	}

	if *compact {
		log.Println("Start Last Compact")
		db.CompactRange(util.Range{})
		log.Println("End Compact")
	}

	log.Println("End")
}

func genData(alg string) ([]byte, []byte) {
	data := randomBytes(4 * 1024)
	ts := time.Now().UnixNano()

	switch alg {
	case "line":
		return []byte(fmt.Sprintf("%0x", ts)), data

	case "random":
		h := md5.New()
		h.Write([]byte(fmt.Sprintf("%0x", ts)))
		return h.Sum(nil), data

	default:
		return []byte{0x0}, []byte{0x0}
	}
}

func randomInt(min, max int) int {
	return min + rand.Intn(max-min)
}

func randomBytes(len int) []byte {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		bytes[i] = byte(randomInt(65, 90))
	}
	return bytes
}
