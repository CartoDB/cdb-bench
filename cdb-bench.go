package main

import (
	"encoding/gob"
	"fmt"
	vegeta "github.com/cartodb/cdb-bench/vegeta/lib" //TODO: don't use monkeypatched Vegeta
	"github.com/spf13/cobra"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Bbox struct {
	min_x int
	min_y int
	max_x int
	max_y int
	zoom  int
}

func (bbox *Bbox) BboxFromString(bbox_str string, zoom int) {
	splitted := strings.Split(bbox_str, ",")
	bbox.zoom = zoom
	bbox.min_x, _ = strconv.Atoi(splitted[0])
	bbox.min_y, _ = strconv.Atoi(splitted[1])
	bbox.max_x, _ = strconv.Atoi(splitted[2])
	bbox.max_y, _ = strconv.Atoi(splitted[3])
	if bbox.max_x == -1 {
		bbox.max_x = int(math.Pow(float64(2), float64(bbox.zoom))) - 1
	}
	if bbox.max_y == -1 {
		bbox.max_y = int(math.Pow(float64(2), float64(bbox.zoom))) - 1
	}
	if len(splitted) == 5 {
		bbox.zoom, _ = strconv.Atoi(splitted[4])
		// Get multiplication factor to scale bbox to:
		bbox.max_y = int(math.Ceil(float64(bbox.max_y) * (math.Pow(float64(2), float64(zoom-bbox.zoom)))))
		bbox.max_x = int(math.Ceil(float64(bbox.max_x) * (math.Pow(float64(2), float64(zoom-bbox.zoom)))))
		bbox.min_y = int(math.Floor(float64(bbox.min_y) * (math.Pow(float64(2), float64(zoom-bbox.zoom)))))
		bbox.min_x = int(math.Floor(float64(bbox.min_x) * (math.Pow(float64(2), float64(zoom-bbox.zoom)))))
	}
}

func file(name string, create bool) (*os.File, error) {
	switch name {
	case "stdin":
		return os.Stdin, nil
	case "stdout":
		return os.Stdout, nil
	default:
		if create {
			return os.Create(name)
		}
		return os.Open(name)
	}
}

func targetFactory(base string, hostname string) vegeta.Targeter {
	return func() (*vegeta.Target, error) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		url := fmt.Sprintf("%s/api/v1/map/%f", base, r.Float64())
		return &vegeta.Target{
			Method: "GET",
			URL:    url,
		}, nil
	}
}

func (boundaries *Bbox) getTile(tile int) (x int, y int) {
	// first, trim by the max of them
	xSize := (boundaries.max_x - boundaries.min_x + 1)
	ySize := (boundaries.max_y - boundaries.min_y + 1)
	maxTrim := xSize * ySize
	tile = tile % maxTrim
	y = (tile / xSize) + boundaries.min_y
	x = (tile % xSize) + boundaries.min_x
	return
}

func tileXYZTargetFactory(base string, hostname string, layergroup string, zoom int, seed int, boundaries Bbox) vegeta.Targeter {
	// We build the hOST in the header
	headers := make(http.Header)
	headers.Add("Host", hostname)
	r := rand.New(rand.NewSource(int64(seed)))
	//boundaries := [][]int{{157, 283}, {354, 436}}
	//boundaries := [][]int{{9, 21}, {17, 27}} //TODO: add a parameter to customize this
	return func() (*vegeta.Target, error) {
		//r := rand.New(rand.NewSource(time.Now().UnixNano()))
		// We reuse the random seed to obtain X,Y in tile range
		x, y := boundaries.getTile(r.Int())
		// We generate random X/Y from 0 to 2^zoom
		url := fmt.Sprintf("%s/api/v1/map/%s/%d/%d/%d.png?_cache_bust=%f", base, layergroup, zoom,
			x, y, r.Float64())
		return &vegeta.Target{
			Method: "GET",
			URL:    url,
			Header: headers,
		}, nil
	}
}

func main() {

	var base string
	var layergroup string
	var hostname string
	var duration int
	var rate int
	var seed int
	var zoom int
	var bbox_string string
	var bbox Bbox

	var BenchCmd = &cobra.Command{
		Use:   "cdb-bench",
		Short: "Benchmark CartoDB",
		Long:  "CartoDB benchmark utility. It will output by default a Vegeta bin report file",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("You must specify a subcommand! See cdb-bench --help")
		},
	}

	var tilesCmd = &cobra.Command{
		Use:   "tiles",
		Short: "Request random tiles in a zoom level given a layergroup",
		Run: func(cmd *cobra.Command, args []string) {

			bbox.BboxFromString(bbox_string, zoom)
			targeter := tileXYZTargetFactory(base, hostname, layergroup, zoom, seed, bbox)
			attacker := vegeta.NewAttacker()

			out, _ := file("stdout", true)
			enc := gob.NewEncoder(out)

			for res := range attacker.WindshaftAttack(targeter, uint64(rate), time.Duration(duration)*time.Second) {
				enc.Encode(res)
			}
		},
	}

	BenchCmd.PersistentFlags().StringVarP(&base, "base", "b", "http://127.0.0.1:8181", "Base host to use")
	BenchCmd.PersistentFlags().StringVarP(&hostname, "hostname", "H", "devuser.localhost.lan", "Host header")
	BenchCmd.PersistentFlags().IntVarP(&duration, "duration", "d", 30, "Duration (in seconds)")
	BenchCmd.PersistentFlags().IntVarP(&rate, "rate", "r", 30, "Requests per second ratio")
	tilesCmd.Flags().IntVarP(&zoom, "zoom", "z", 10, "Zoom level")
	tilesCmd.Flags().IntVarP(&seed, "seed", "s", int(time.Now().UnixNano()), "Random seed (default value is unix time in msecs)")
	tilesCmd.Flags().StringVarP(&layergroup, "layergroup", "l", "0:0", "Layergroup")
	tilesCmd.Flags().StringVarP(&bbox_string, "bbox", "x", "0,0,-1,-1", "Bounding box, XYXY (add a fitfth parameter to specify zoom if you want it to be translated to other zoom)")

	BenchCmd.AddCommand(tilesCmd)
	BenchCmd.Execute()

	//metrics := vegeta.NewMetrics(results)
	//fmt.Printf("99th percentile: %s\n", metrics.Latencies.P99)
}
