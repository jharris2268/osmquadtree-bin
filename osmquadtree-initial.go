package main

import (
    "github.com/jharris2268/osmquadtree/locationscache"
    "github.com/jharris2268/osmquadtree/readfile"
    "github.com/jharris2268/osmquadtree/elements"
    
    "runtime"
    "time"
    "strings"
    "flag"
    "fmt"
    "encoding/json"
    "os"
)


type UpdateSettings struct {
	SourcePrfx    string
	DiffsLocation string
	InitialState  int64
	RoundTime     bool
}
    
const defaultSource = string("http://planet.openstreetmap.org/replication/day/")

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU()*2)
    
    
    infnn := flag.String("i","","input pbf file")
    prfx  := flag.String("p","planet/", "prefix")
    eds   := flag.String("e", "", "timestamp")
    initialState  := flag.Int("s",0,"initial state")
    sourcePrfx    := flag.String("sourceprfx", defaultSource, "source (defaults to "+defaultSource+")")
	diffsLocation := flag.String("diffslocation", "", "diffs location: where osc.gz files are saved")
	roundTime     := flag.Bool("roundtime", false, "round timestamp up to nearest day")
    
    flag.Parse()
    
    endDate,err := elements.ReadDateString(*eds)
    if err!=nil {
         panic(err.Error())
    }
    
    infn := *infnn
    
    if infn == "" {
        infn = *eds+".pbf"
    } else if strings.HasPrefix(infn, *prfx) {
        infn = strings.Replace(infn, *prfx, "", 1)
    }
    
    //infn := endstr+".pbf"
   // state=812
    
    fmt.Println(*prfx,infn,endDate,*initialState)
    
    st:=time.Now()
    //df := readfile.MakeProcessPbfFile(*prfx+infn,4)
    fn:=*prfx+infn
    fmt.Println("import",fn)
    
    df, err := readfile.ReadExtendedBlockMulti(*prfx+infn,4)
    if err!=nil {
        panic(err.Error())
    }
    
    err = locationscache.MakeLocationsCache(df,infn,*prfx,int64(endDate),int64(*initialState))
    
    if err!=nil {
        panic(err.Error())
    }
    fmt.Printf("took %8.1fs\n", time.Since(st).Seconds())
    
    us := UpdateSettings{}
    us.SourcePrfx = *sourcePrfx
    if *diffsLocation != "" {
        us.DiffsLocation = *diffsLocation
    } else {
        us.DiffsLocation = *prfx + "diffs/"
    }
    us.InitialState = int64(*initialState)
    us.RoundTime = *roundTime
    
    settingsf, err := os.Create(*prfx+"settings.json")
    if err!=nil { panic(err.Error()) }
    defer settingsf.Close()
    err = json.NewEncoder(settingsf).Encode(us)
    if err!=nil { panic(err.Error()) }
    
}
    
