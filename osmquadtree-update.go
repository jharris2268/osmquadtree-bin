/*
Copyright 2015 James Harris

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
    "github.com/jharris2268/osmquadtree/elements"
	"github.com/jharris2268/osmquadtree/locationscache"
	"github.com/jharris2268/osmquadtree/update"
	"github.com/jharris2268/osmquadtree/utils"
    "github.com/jharris2268/osmquadtree/writefile"


    "bufio"
	"bytes"
    
    "errors"
	"flag"
	"fmt"
    "io"
	"io/ioutil"
    "net/http"
	"log"
	"os"
    "runtime"
    "strconv"
	"strings"
    "time"
    "encoding/csv"
)

func fetchDiff(dst, src string) (int64, float64, error) {
	resp, err := http.Get(src)
	if err != nil {
		return 0, 0, err
	}

	defer resp.Body.Close()

	outfn, err := os.Create(dst)
	if err != nil {
		return 0, 0, err
	}

	defer outfn.Close()
    
    st:=time.Now()
	ln, err := io.Copy(outfn, resp.Body)
	outfn.Sync()
	if err != nil {
		return 0, 0, err
	}
	return ln, time.Since(st).Seconds(), nil
}

func getStateFile(rt string) (int64, elements.Timestamp, error) {

	resp, err := http.Get(rt + "state.txt")
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, err
	}

	body := bufio.NewScanner(bytes.NewReader(data))

	sn := int64(0)
	ts := time.Time{}
    //ts := elements.Timestamp(0)

	i := 0
	for body.Scan() {
		tx := body.Text()
		//fmt.Printf("%-2d %q\n",i,tx)
		i++
		ln := strings.Split(tx, "=")
		if len(ln) == 2 {
			var err error
			switch ln[0] {
			case "sequenceNumber":
				sn, err = strconv.ParseInt(ln[1], 10, 64)
			case "timestamp":
				//ts, err = elements.ReadDateString(ln[1])
                ts,err = time.Parse(`2006-01-02T15\:04\:05Z`, ln[1])
			}
			if err != nil {
				return 0, 0, err
			}
		}
	}
	if sn != 0 && !ts.IsZero() {
		//
		return sn, elements.Timestamp(ts.Unix()), nil
	}
	return 0, 0, errors.New(fmt.Sprintf("couldn't find data: %q", string(data)))
}



type updateSpec struct {
	srcFile   string
	timestamp elements.Timestamp
	outFile   string
	state     int64
    seconds   float64
    
}

func (o updateSpec) String() string {
    a:= fmt.Sprintf("%-40s %-20s %4d => %-25s", o.srcFile,o.timestamp,o.state,o.outFile)
    if o.seconds>0 {
        a += fmt.Sprintf(": %8.1fs", o.seconds)
    }
    return a
    
}

func makeOutFilename(prfx string, ts elements.Timestamp, rt bool) string {
    tss := ts.FileString(rt)
    return fmt.Sprintf("%s%s.pbfc", prfx, tss)
    
}

func makeDiffUrl(prfx string, state int64) string {
	a := state / 1000000
	b := (state / 1000) % 1000
	c := state % 1000

	return fmt.Sprintf("%s/%03d/%03d/%03d", prfx, a, b, c)
}

func makeStateFn(prfx string, state int64) string {
    if state<1000 {
        return fmt.Sprintf("%s%03d.osc.gz", prfx, state)
    }
    return fmt.Sprintf("%s%d.osc.gz", prfx, state)
        
}

func getState(srcprfx string, diffslocation string, state int64) (int64, elements.Timestamp, error) {
    sf,err := os.Open(diffslocation + "state.csv")
    hasfile := false
    if err!= nil {
        //pass
    } else {
        defer sf.Close()
        hasfile=true
    } 
    
    if state>0 {
        
        
        if hasfile {
            //fmt.Println("have file")
            
            ff := csv.NewReader(sf)
            var lr []string
            for {
                rec,err := ff.Read()
                if err == io.EOF {
                    break
                }
                if err != nil || len(rec) !=2 {
                    log.Fatal(err)
                }
                //fmt.Println(rec)
                sn, err := strconv.ParseInt(rec[0], 10, 64)
                if err!=nil { log.Fatal(err) }
                ts,err := time.Parse(`2006-01-02T15-04-05`, rec[1])
                if err!=nil { log.Fatal(err) }
                
                if sn == state {
                    
                    return sn,elements.Timestamp(ts.Unix()),nil
                }
                lr=rec
            }
            fmt.Println("couldn't find",state,"lastrow",lr)
        }
    }
            
    
	fl := srcprfx
	if state >= 0 {
		fl = makeDiffUrl(srcprfx, state)
		if !strings.HasSuffix(fl, ".") {
			fl += "."
		}
	}
    
    sn,ts,err := getStateFile(fl)
    if err!=nil {
        return sn,ts,err
    }
    
    if state>0 && hasfile {
        sf2,err :=os.OpenFile(sf.Name(), os.O_RDWR|os.O_APPEND, 0666)
        if err != nil {
            log.Println("??",err.Error())
        } else {
            fmt.Println(state,"ADD to",sf.Name(),sn,ts)
            ww := csv.NewWriter(sf2)
            nl := []string{fmt.Sprintf("%d",sn), ts.FileString(false)}
            
            ww.Write(nl)
            ww.Flush()
            //sf2.Flush()
            sf2.Close()
        }
    }
    return sn,ts,err
}

const secondsInDay = elements.Timestamp(24 * 60 * 60)

/*
func fetchDiff(src string, dst string) (int64, error) {
    
    fmt.Println("call wget -o %s %s", dst,src)
    return 0, errors.New("CALL")
}
    
func getStateFile(src string) (int64, elements.Timestamp, error) {
    return 0,0,nil
}*/

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU()*2)
    
  	outputPrfxp := flag.String("p", "", "out prefix")

	endDate     := flag.String("e", "", "end date")
	updateFn    := flag.String("u", "", "update file name")
	state       := flag.Int("s", 0, "new state number")
	outFileName := flag.String("o", "", "output file name")
	roundTimep  := flag.Bool("roundTime", false, "round timeStamp up to nearest day")

    flag.Parse()
    
    outputPrfx := *outputPrfxp
    
	if len(os.Args) == 2 {
		outputPrfx = os.Args[1]
	}

	if outputPrfx == "" {
		fmt.Println("must specify prfx")
		os.Exit(1)
	}

	
	settings, err := locationscache.GetUpdateSettings(outputPrfx)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("settings file doesn't exist: rely on specified flags")
		} else {
			fmt.Println("open settings.json", err.Error())
			os.Exit(1)
		}
	}
    roundTime := (settings.RoundTime || *roundTimep)


	ops := []updateSpec{}

	if *updateFn != "" {
		ed, err := elements.ReadDateString(*endDate)
		if err != nil {
			fmt.Println("getTimestamp", err.Error())
			os.Exit(1)
		}
		ofn := ""
		//*outputPrfx + *outFileName
		if *outFileName == "SKIP" {
			//pass
		} else if *outFileName == "" {
			ofn = makeOutFilename(outputPrfx, ed, roundTime)
		} else {
			ofn = outputPrfx + *outFileName
		}

		ops = []updateSpec{updateSpec{*updateFn, ed, ofn, int64(*state),0.0}}
	} else {
		if settings.SourcePrfx == "" {
			fmt.Println("if no settings must specify output file")
		}
		writefile := true

		if *outFileName == "" {
			//pass
		} else if *outFileName == "SKIP" {
			writefile = false
		} else {
			fmt.Println("incompatable: settings file && outFileName")
			os.Exit(1)
		}

		currentState, _, err := getState(settings.SourcePrfx, settings.DiffsLocation, -1)
		if err != nil {
			fmt.Println("getState", err.Error())
			os.Exit(1)
		}
		lastState,err := locationscache.GetLastState(outputPrfx,settings.LocationsCache)
        if err!=nil { 
            fmt.Println("locationscache.GetLastState", err.Error())
			os.Exit(1)
		}
		if currentState <= lastState {
			log.Println("Nothing to do")
			os.Exit(0)
		}

		for state := lastState + 1; state < currentState+1; state++ {
			statefn := makeStateFn(settings.DiffsLocation, state)

			fe, err := utils.FileExists(statefn)
			if err != nil {
				fmt.Println("check FileExists", statefn, err.Error())
				os.Exit(1)
			}
			fmt.Println(state, statefn, fe)
			if !fe {
				log.Printf("fetching %s", statefn)
				ln, tt, err := fetchDiff(statefn, makeDiffUrl(settings.SourcePrfx, state)+".osc.gz")
				if err != nil {
					fmt.Println("fetching file", state, err.Error())
					os.Exit(1)
				}
				log.Printf("fetched %6.1f kb in %8.1fs\n", float64(ln)/1024.0, tt)

			}
			_, ts, err := getState(settings.SourcePrfx, settings.DiffsLocation, state)
			if err != nil {
				fmt.Println("getState", state, err.Error())
				os.Exit(1)
			}
			if (ts%secondsInDay) != 0 && roundTime {
				ots := ts.String()
				if ts%secondsInDay < 7200 {
					ts = (ts / secondsInDay) * secondsInDay
				}
				ts = (ts/secondsInDay + 1) * secondsInDay
				fmt.Printf("Round %s => %d %s\n", ots, ts, ts.DateString())

			}

			ofn := ""
			if writefile {
				ofn = makeOutFilename(outputPrfx, ts, roundTime)
			}
			ops = append(ops, updateSpec{statefn, ts, ofn, state,0.0})
		}
	}
	//fmt.Println(ops)
    
	for i, o := range ops {
        st:=time.Now()
		fmt.Println(o)
		zz := o.outFile
		if strings.HasPrefix(zz, outputPrfx) {
			zz = strings.Replace(zz, outputPrfx, "", 1)
		}

		log.Printf("call CalcUpdateTiles(%q, %q, %d, %q, %d)", outputPrfx, o.srcFile, o.timestamp, zz, o.state)
		blcks,qts, err := update.CalcUpdateTiles(outputPrfx, o.srcFile, o.timestamp, zz, o.state,settings.LocationsCache, false, settings.IncludeUnchangedNodes)
		if err != nil {
			fmt.Println("CalcUpdateTiles", err.Error())
			os.Exit(1)
		}
		if o.outFile == "" {
			for _,bl:= range blcks {
                go func() {
                    for range bl {
                    }
                }()
                
			}
		} else {
            fmt.Println("updated",len(qts),"tiles")
            
            idx,err := writefile.WritePbfFile(blcks,o.outFile,true,settings.QuadtreeTuple)
            
			
			if err != nil {
				fmt.Println("writeFileIndexed", o.outFile, err.Error())
				os.Exit(1)
			}
			log.Printf("wrote %d items, %d bytes to %s\n", idx.Len(), 0, o.outFile)

		}
        ops[i].seconds=time.Since(st).Seconds()
	}

    for _,o:=range ops {
        fmt.Println(o)
    }
	os.Exit(0)
}
