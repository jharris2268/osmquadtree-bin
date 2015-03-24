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
    "github.com/jharris2268/osmquadtree/locationscache"
    "github.com/jharris2268/osmquadtree/readfile"
    "github.com/jharris2268/osmquadtree/elements"
    
    "runtime"
    "time"
    "strings"
    "flag"
    "fmt"
)




func main() {
    runtime.GOMAXPROCS(runtime.NumCPU()*2)
    
    
    infnn := flag.String("i","","input pbf file")
    prfx  := flag.String("p","planet/", "prefix")
    eds   := flag.String("e", "", "timestamp")
    initialState  := flag.Int("s",0,"initial state")
    sourcePrfx    := flag.String("sourceprfx", locationscache.DefaultSource, "source (defaults to "+locationscache.DefaultSource+")")
	diffsLocation := flag.String("diffslocation", "", "diffs location: where osc.gz files are saved")
	roundTime     := flag.Bool("roundtime", false, "round timestamp up to nearest day")
    lctype        := flag.String("l", "pbf", "location cache type")
    
    flag.Parse()
    
    endDate,err := elements.ReadDateString(*eds)
    if err!=nil {
         panic(err.Error())
    }
    
    infn := *infnn
    
    if infn == "" {
        infn = endDate.FileString(*roundTime)+".pbf"
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
    
    df, err := readfile.ReadQtsMulti(*prfx+infn,4)
    if err!=nil {
        panic(err.Error())
    }
    
    err = locationscache.MakeLocationsCache(df,*lctype,infn,*prfx,endDate,int64(*initialState))
    
    if err!=nil {
        panic(err.Error())
    }
    fmt.Printf("took %8.1fs\n", time.Since(st).Seconds())
    
    us := locationscache.UpdateSettings{}
    us.SourcePrfx = *sourcePrfx
    if *diffsLocation != "" {
        us.DiffsLocation = *diffsLocation
    } else {
        us.DiffsLocation = *prfx + "diffs/"
    }
    us.InitialState = int64(*initialState)
    us.RoundTime = *roundTime
    
    us.LocationsCache = *lctype
    
    err = locationscache.WriteUpdateSettings(*prfx,us)
    if err!=nil { panic(err.Error()) }
}
    
