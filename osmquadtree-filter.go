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
    "github.com/jharris2268/osmquadtree/readfile"
    "github.com/jharris2268/osmquadtree/quadtree"
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/utils"
    
    "github.com/jharris2268/osmquadtree/locationscache"
    
    "github.com/jharris2268/osmquadtree/blocksort"
    "github.com/jharris2268/osmquadtree/writefile"
    "github.com/jharris2268/osmquadtree/filter"
    
    "io"
    "runtime"
    //"runtime/debug"
    "strings"
    "encoding/json"
    "errors"
    "os"
    "fmt"
    //"unsafe"
    //"time"
    //"sort"
    //"regexp"
    //"math"
    "strconv"
    "net/http"
    "flag"
)


func readBbox(f string) *quadtree.Bbox {
    if f=="planet" {
        return quadtree.PlanetBbox()
    }
    t:=strings.Split(f,",")
    if len(t)!=4 {
        return quadtree.NullBbox()
    }
    
    mx,_,err := utils.ParseStringInt(t[0])
    if err!=nil { return quadtree.NullBbox() }
    my,_,err := utils.ParseStringInt(t[1])
    if err!=nil { return quadtree.NullBbox() }
    Mx,_,err := utils.ParseStringInt(t[2])
    if err!=nil { return quadtree.NullBbox() }
    My,_,err := utils.ParseStringInt(t[3])
    if err!=nil { return quadtree.NullBbox() }
    return &quadtree.Bbox{mx,my,Mx,My}
    
}

func process_filter(
    srcfn string, chgfns []string, endDate elements.Timestamp,
    qq quadtree.QuadtreeSlice,
    locTest filter.LocTest,
    merge bool, trim bool, sort bool, 
    out io.Writer) (int64, error) {

    passQt := func(q quadtree.Quadtree) bool { return true }
    
    
    qs := map[quadtree.Quadtree]bool{}
    ql:=make([]quadtree.Quadtree,0,len(qq))
    for _,q:=range qq {
        if locTest.IntersectsQuadtree(q) {
            qs[q]=true
            ql=append(ql,q)
        }
        
    }
    
    fmt.Printf("have %d qts\n", len(qs))
    passQt = readfile.MakePassQt(qs)

    isBig := (len(ql)>=1000)
    
    var readData func() ([]chan elements.ExtendedBlock,error)
    
    if merge || sort {
        readData = func() ([]chan elements.ExtendedBlock,error) {
            return readfile.ReadExtendedBlockMultiMergeQts(srcfn, chgfns, 4, passQt)
            
        }
    } else {
        readData = func() ([]chan elements.ExtendedBlock,error) {
            return readfile.ReadExtendedBlockMultiQtsUnmerged(srcfn, chgfns, 4, passQt, 4)
        }
    }
    
    if trim {
        readDataOrig := readData
        readData = func() ([]chan elements.ExtendedBlock,error) {
            
            
            ids := filter.MakeIdSet(isBig)
            dd,err := readDataOrig()
            if err!=nil { return nil,err}
            ddc := readfile.CollectExtendedBlockChans(dd,false)
            err = filter.FindObjsFilter(ddc, locTest, ids)
            if err!=nil { return nil, err }
            dd,err = readDataOrig()
            if err!=nil { return nil,err}
            return filter.FilterObjs(dd, ids)
        }
    }
    
    if sort {
        readDataOrig := readData
        readData = func() ([]chan elements.ExtendedBlock,error) {
            inBlcks,err := readDataOrig()
            if err!=nil { return nil,err }

            st := "inmem"
            if isBig {
                st = "tempfilesplit"
            }
            
            return blocksort.SortElementsById(inBlcks, 4, endDate, 8000, st)
        }
    }
    
    
    data,err := readData()
    if err!=nil {
        return 0, err
    }
    
    dataCollected := readfile.CollectExtendedBlockChans(data,false)
    
    _,err= writefile.WritePbfIndexed(dataCollected, out, nil, false, false, sort)
    return 0,err
    
    
}
func asFloat(ins string) float64 {
	f, err := strconv.ParseFloat(ins, 64)
	if err != nil {
		return 0
	}
	return f
}

func asInt(ins string) int64 {
    f,err := strconv.ParseInt(ins,10,64)
    if err!=nil {
        return 0
    }
    return f
}




func makeLocTestPoly(r *http.Request) (filter.LocTest,error) {
    
    
    fmt.Println(r.Form)
    
    lons := make([]int64, len(r.Form["lon[]"]))
    lats := make([]int64, len(r.Form["lat[]"]))
    if len(lons)!=len(lats) { return nil,errors.New("len(lons)!=len(lats)") }

    //ok := true
    for i,ln := range r.Form["lon[]"] {
        lons[i] = utils.Intm(asFloat(ln))
        //if !ok { return nil,errors.Error(fmt.Sprintf("%q not a float", ln))  }
    }
    for i,lt := range r.Form["lat[]"] {
        lats[i] = utils.Intm(asFloat(lt))
        //if !ok { return nil,errors.Error(fmt.Sprintf("%q not a float", lt)) }
    }
    
    return filter.MakeLocTestPolygon(lons,lats),nil
}

func makeLocTest(request *http.Request) (filter.LocTest,error) {
    if request.FormValue("lon[]") != "" {
        fmt.Println(request.FormValue("lon[]"))
        return makeLocTestPoly(request)
    }
    
    box := quadtree.Bbox{}
                    
    if request.Form.Get("minlon")!="" {
    
        minlon := asFloat(request.Form.Get("minlon"))
        minlat := asFloat(request.Form.Get("minlat"))
        maxlon := asFloat(request.Form.Get("maxlon"))
        maxlat := asFloat(request.Form.Get("maxlat"))
        box.Minx, box.Miny = quadtree.ToInt(minlon), quadtree.ToInt(minlat)
        box.Maxx, box.Maxy = quadtree.ToInt(maxlon), quadtree.ToInt(maxlat)
        
        
    } else if request.Form.Get("tilex") != "" {
        
        tx:=asInt(request.Form.Get("tilex"))
        ty:=asInt(request.Form.Get("tiley"))
        tz:=asInt(request.Form.Get("tilez"))
          
        qt,err:=quadtree.FromTuple(tx,ty,tz)
        if err!=nil { return nil,err }
        box = qt.Bounds(0.0)
        fmt.Println(tx,ty,tz,"=>",box)
    } else {
        return nil,errors.New("No bounds set")
    }
    
    return filter.AsLocTest(box),nil
}


type filterData struct {
    srcfn string
    chgfns []string
    endDate elements.Timestamp
    qq quadtree.QuadtreeSlice
    files []FileInfo
}

type FileInfo struct {
    FileName string
    StartDate string
    EndDate string
    NumTiles int
}

func makeFileInfo(srcfn string, chgfns []string, ed elements.Timestamp, qq quadtree.QuadtreeSlice) []FileInfo {
    res := make([]FileInfo, 1+len(chgfns))
    res[0].FileName=srcfn
    res[0].StartDate=""
    
    for i,c:=range chgfns {
        res[i+1].FileName = c
    }
    return res
}
    
func filterQts(lc filter.LocTest, qts quadtree.QuadtreeSlice) quadtree.QuadtreeSlice {
    res:=make(quadtree.QuadtreeSlice, 0, len(qts))
    for _,q:=range qts {
        if lc.IntersectsQuadtree(q) {
            res=append(res, q)
        }
    }
    return res
}



func (fd *filterData) infoJson(w http.ResponseWriter, r *http.Request) {
    //log.Println("infoJson")
    
    
    
    ii := map[string]interface{} {}
    
    err := func() error {
        err:=r.ParseForm()
        if err!=nil { return err }
        
        locTest,err := makeLocTest(r)
        if err!=nil { return err }
        
        
        ii["files"] = fd.files
        
        ii["qts"] = quadtree.MakeQuadtreeInfo(filterQts(locTest,fd.qq))
        w.Header().Set("Content-Type", "application/json")
        return json.NewEncoder(w).Encode(ii)
    }()
    if err != nil {
		fmt.Println(err.Error())
		http.Error(w, err.Error(), 500)
	}
    
}

func (fd *filterData) process_filter_serve(responseWriter http.ResponseWriter, request *http.Request) {
    
    
    err := func() error {
        err := request.ParseForm()
        if err != nil { return nil }
        
        locTest,err := makeLocTest(request)
        if err != nil { return nil }
        
        fmt.Println(request.Form)
        
        trim := request.Form.Get("trim")=="true"
        merge := request.Form.Get("merge")=="true"
        sort := request.Form.Get("sort")=="true"
        fn := request.Form.Get("filename")
        
        responseWriter.Header().Set("Content-Type", "application/pbf")
        
        cookie := &http.Cookie{}
        cookie.Name="fileDownload"
        cookie.Value="true"
        cookie.Path ="/"
        http.SetCookie(responseWriter,cookie)
        responseWriter.Header().Set("Content-Disposition", "attachment; filename=\""+fn+"\"")
                    
                
        ln,err := process_filter(
            fd.srcfn,fd.chgfns,fd.endDate,
            fd.qq, locTest, trim,merge,sort,responseWriter)
        
        if err!=nil {
            fmt.Printf("returning %d bytes\n", ln)
        }
        return err
        
    }()
    
    if err != nil {
        fmt.Println(err.Error())
        http.Error(responseWriter, err.Error(), 500)
    }
    
}

    

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU()*2)

    
    prfx := flag.String("p","","prfx")
    endstr := flag.String("e","","enddate")
    filt   := flag.String("f","planet","filter")
    merge  := flag.Bool("merge",false,"merge changes")
    trim   := flag.Bool("trim", false, "trim to box")
    sort   := flag.Bool("sort", false, "sort objs")
    outfn  := flag.String("o", "", "out filename")
    serve  := flag.Bool("s", false, "server")
    
    flag.Parse()
    
    if (*prfx)=="" {
        panic("must specify file prefix")
    }
    
    
    endDate := elements.Timestamp(0)
    if (*endstr)!="" {
        var err error
        endDate,err = elements.ReadDateString(*endstr)
        if err!=nil {
             panic(err.Error())
        }
    }
    
    origfn := *prfx
    var qq quadtree.QuadtreeSlice
    var chgfns []string
    
    if strings.HasSuffix(origfn, ".pbf") {
        
        _,ii,err := readfile.GetHeaderBlock(origfn)
        if err!=nil { panic(err.Error()) }
        qq = make(quadtree.QuadtreeSlice, ii.Index.Len())
        for i,_:=range qq {
            qq[i]=ii.Index.Quadtree(i)
        }
        
        
    } else {
        var ii []locationscache.IdxItem
        var err error
        ii,qq,err = locationscache.GetCacheSpecs(*prfx)        
        if err!=nil {
            panic(err.Error())
        }
        
        origfn = *prfx+ii[0].Filename
        
        chgfns = make([]string,0,len(ii)-1)
        if len(ii)>1 {
            for _,i := range ii[1:] {
                chgfns = append(chgfns,*prfx+i.Filename)
                if (endDate!=0) && (i.Timestamp >= endDate) {
                    break
                }
            }
            if len(chgfns) > 0 {
                fmt.Printf("origfn:%s, %d changes [%s=>%s]\n",origfn, len(chgfns), chgfns[0], chgfns[len(chgfns)-1])
            }
        }
    }
    
        
    
    
    if *outfn != "" {
        
        
        var locTest filter.LocTest
        
        if *filt!="" {
            if strings.HasSuffix(*filt, ".poly") {
                var err error
                locTest,err = filter.ReadPolyFile(*filt)
                if err!=nil { panic(err.Error()) }
            } else {
            
                fbx := readBbox(*filt)
                locTest = filter.AsLocTest(*fbx)
                fmt.Println(fbx)
            }
        }
        if locTest==nil {
            fbx := quadtree.PlanetBbox()
            locTest = filter.AsLocTest(*fbx)
        }
        
        outF,err := os.Create(*outfn)
        if err!=nil { panic(err.Error()) }
        
        ln, err := process_filter(origfn,chgfns, endDate, qq, locTest, *merge, *trim, *sort, outF)
        if err!=nil { panic(err.Error()) }
        fmt.Printf("wrote %d bytes to %s\n", ln, outF.Name())
        outF.Close()
    }
    
    if (*serve) {
        
        fd := &filterData{origfn,chgfns,endDate, qq, makeFileInfo(origfn,chgfns,endDate,qq) }
        
        http.HandleFunc("/filter", fd.process_filter_serve)
        http.HandleFunc("/info", fd.infoJson)
        http.HandleFunc("/",returnIndex)
        
        fmt.Println("Listening... on localhost:17832")
        panic(http.ListenAndServe(":17832", nil))
    }

}
