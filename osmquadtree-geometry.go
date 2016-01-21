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
    "github.com/jharris2268/osmquadtree/geometry"
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/utils"
    
    "github.com/jharris2268/osmquadtree/locationscache"
    "github.com/jharris2268/osmquadtree/calcqts"
    "github.com/jharris2268/osmquadtree/filter"
    "github.com/jharris2268/osmquadtree/write"
    "github.com/jharris2268/osmquadtree/geojson"
    "github.com/jharris2268/osmquadtree/sqlselect"
    "github.com/jharris2268/osmquadtree/writefile"
    "github.com/jharris2268/osmquadtree/blocksort"
    
    "github.com/jharris2268/osmquadtree/packeddatastore"
    
    "runtime"
    "runtime/debug"
    "strings"
    "encoding/json"
    "errors"
    "os"
    "fmt"
    //"unsafe"
    "time"
    "sort"
    "regexp"
    //"math"
    "strconv"
    "net/http"
    "flag"
    "sync"
)

var cleanQueryString *regexp.Regexp



 

func hasTag(tags elements.Tags, k string) bool {
    if tags==nil {
        return false
    }
    tt,ok := tags.(geometry.TagsEditable)
    if ok {
        return tt.Has(k)
    }
    for i:=0; i<tags.Len(); i++ {
        if tags.Key(i)==k {
            return true
        }
    }
    return false
}



type tabQuPair struct {
    query sqlselect.Tabler
    text  string
}




func readQueriesFn(mmlfn string) (map[int]tabQuPair,error) {
    fmt.Println("readQueriesFn",mmlfn)
    mml,err :=os.Open(mmlfn)
    if err!=nil {
        return nil,err
    }
    mmlj := map[string]interface{}{}
    err = json.NewDecoder(mml).Decode(&mmlj)
    if err!=nil {
        return nil,err
    }
    
    
    qs := map[int]string{}
    
    
    
    if _,ok := mmlj["Layer"]; ok {
    
        lays := mmlj["Layer"].([]interface{})
        
        for i,l := range lays {
            ds := l.(map[string]interface{})["Datasource"].(map[string]interface{})
            
            if t,ok := ds["type"]; ok && (t=="python" || t=="postgis") {
                tab := ds["table"].(string)
                //hp:=strings.Contains(tab,"pixel")
                qs[i]=tab
                
            }
        }
    } else {
        for k,v := range mmlj {
            i,err := strconv.Atoi(k)
            if err!=nil { return nil,err }
            qs[i]=v.(string)
        }
    }
        
    qq := map[int]tabQuPair{}
    
    for i,tab := range qs {
        tab=strings.Replace(tab,"/(!pixel_width!*!pixel_height!)","",-1)
        tab=strings.Replace(tab,"!pixel_width!", "1",-1)
        tab=strings.Replace(tab,"!pixel_height!", "1",-1)
        tab=cleanQuery(tab)
        /*if hp {
            fmt.Println(i,tab)
        }*/
        tabl,err := sqlselect.Parse(tab)
        if err!=nil {
            fmt.Println("Problem with",tab,err.Error(),"\n")
        } else {
            qq[i] = tabQuPair{tabl,tab}
        }
    }
    
    
    
    fmt.Println("have",len(qq),"queries:")
    return qq,nil
}

func readViewsFn(viewsFn string) ([]tableSpec, error) {
    fmt.Println("readViewsFn",viewsFn)
    fl,err :=os.Open(viewsFn)
    if err!=nil {
        return nil,err
    }
    viewsData := map[string]interface{}{}
    err = json.NewDecoder(fl).Decode(&viewsData)
    if err!=nil {
        return nil,err
    }
    
    result := make([]tableSpec, 0, len(viewsData))
    for k,vv := range viewsData {
        v:=vv.(map[string]interface{})
        spec := tableSpec{}
        spec.name = k
        //vm := v.(map[string]string)
        gt,ok := v["type"]
        if ok {
            gts,ok := gt.(string)
            if !ok { return nil, errors.New(fmt.Sprintf("?? %q",gt))}
            
            switch gts {
                case "POINT": spec.geomtype = geometry.Point
                case "LINESTRING": spec.geomtype = geometry.Linestring
                case "POLYGON": spec.geomtype = geometry.Polygon
                case "GEOMETRYCOLLECTION": spec.geomtype = geometry.Multi
            }
        }
        tab,ok := v["table"]
        if !ok { return nil, errors.New("no table") }
        tabs,ok := tab.(string)
        if !ok { return nil, errors.New(fmt.Sprintf("table not a string?? %q",tab)) }
        query,err := sqlselect.Parse(tabs)
        if err!=nil { return nil,err}
        spec.query=query
        result=append(result,spec)
        //spec.cols =query.Columns()
    }
    return result,nil
}
    


type tableSpec struct {
    name string
    geomtype geometry.GeometryType
    cols []string
    query sqlselect.Tabler
}

type tablesQueryResult struct {
    bbox   quadtree.Bbox
    qt     quadtree.Quadtree
    tables map[string]sqlselect.Result
}



type tablesQuery struct {
    //ts map[quadtree.Quadtree][]objRow
    pds packeddatastore.PackedDataStore
    
    spec []tableSpec
    queries map[int]tabQuPair
    queriesalt []tabQuPair
    prev map[int]tablesQueryResult
    mi int
    mx int
    xx int
    
    lock sync.Mutex
}

func newTablesQuery(pds packeddatastore.PackedDataStore, spec []tableSpec, queries map[int]tabQuPair) *tablesQuery {
    return &tablesQuery{pds,spec,queries,make([]tabQuPair,0,16),map[int]tablesQueryResult{},-1,0,0,sync.Mutex{}}
}

func (tq *tablesQuery) tables(bb quadtree.Bbox) (map[string]sqlselect.Result,error) {
    for _,v:=range tq.prev {
        if v.bbox == bb {
            return v.tables,nil
        }
    }
    if len(tq.prev)>=1 {
        for len(tq.prev)>=1 {
            delete(tq.prev,tq.mi)
            tq.mi++
        }
        debug.FreeOSMemory()
        fmt.Println("have",len(tq.prev),tq.mi,tq.mx,utils.MemstatsStr())
    }
    
    
    nr,err := make_tables(tq.pds,bb,quadtree.Null,tq.spec)
    
    
    if err!=nil {
        return nil,err
    }
    
    tq.prev[tq.mx] = tablesQueryResult{bb,quadtree.Null,nr}
    tq.mx++
    return nr,nil
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
        
func (tq *tablesQuery) result(idx int, qui int, bb quadtree.Bbox, qu sqlselect.Tabler) (elements.ExtendedBlock, error) {
    
    tabs,err := tq.tables(bb)
    if err!=nil {
        return nil,err
    }
        
    return prep_result(tabs, idx,qui,qu,quadtree.Null,true)
}

func (tq *tablesQuery) rawtile(idx int, qui int, qt quadtree.Quadtree, qu sqlselect.Tabler) (elements.ExtendedBlock, error) {
    nr,err := func() (map[string]sqlselect.Result, error) {
    
        for _,v:=range tq.prev {
            if v.qt == qt {
                return v.tables,nil
            }
        }
        if len(tq.prev)>=3 {
            for len(tq.prev)>=3 {
                delete(tq.prev,tq.mi)
                tq.mi++
            }
            debug.FreeOSMemory()
            fmt.Println("have",len(tq.prev),tq.mi,tq.mx,utils.MemstatsStr())
        }
        
        
        nr,err := make_tables(tq.pds,quadtree.Bbox{}, qt,tq.spec)
        
        
        if err!=nil {
            return nil,err
        }
        
        tq.prev[tq.mx] = tablesQueryResult{quadtree.Bbox{},qt,nr}
        tq.mx++
        return nr,nil
    }()
    if err!=nil { return nil,err}
    return prep_result(nr,idx,qui,qu,qt,false)
}

func prep_result(tabs map[string]sqlselect.Result, idx int, qui int, qu sqlselect.Tabler, qt quadtree.Quadtree, addTags bool) (elements.ExtendedBlock, error) {
    
    rr,err := qu.Result(tabs)
    if err!=nil { return nil,err }
    
    bl := make(elements.ByElementId,rr.Len())
    for i,_ := range bl {
        bl[i] = packeddatastore.MakeRowAsElement(i,rr.Row(i))
    }
    ts:=""
    if rr.Len()>0 {    
        r0 := rr.Row(0)
        kk:=make([]string,r0.Len());
        for i,_ :=range kk {
            kk[i] = r0.Key(i)
        }
        ts = strings.Join(kk, ";")
        //fmt.Println("tags=",ts)
    }
    var tags elements.Tags
    if addTags {
        tags=elements.MakeTags([]string{"idx","query","tags"},[]string{fmt.Sprintf("%d",qui),qu.String(),ts})
    } else {
        tags=elements.MakeTags([]string{"query"},[]string{qu.String()})
    }
    ble := elements.MakeExtendedBlock(idx,bl,qt,0,0,tags)
    return ble,nil
}

func cleanQuery(qu string) string {
    a := cleanQueryString.ReplaceAllString(qu," ")
    b := strings.TrimSpace(a)
    //return strings.ToLower(b)
    return b
}


func (tq *tablesQuery) tiles(w http.ResponseWriter, r *http.Request) {
    err := func() error {
        keys := tq.pds.Keys()
        result := map[string]interface{} {}
        
        for _,k := range keys {
            info := map[string]interface{} {}
            info["qt"] = int64(k)
            x,y,z := k.Tuple()
            info["tuple"] = []int64{x,y,z}
            
            bx := k.Bounds(0.0)
            info["bbox"] = []float64{
                quadtree.ToFloat(bx.Minx),
                quadtree.ToFloat(bx.Miny),
                quadtree.ToFloat(bx.Maxx),
                quadtree.ToFloat(bx.Maxy),
            }
            result[k.String()] = info
        }
        
        w.Header().Set("Content-Type", "application/json")
        
        return json.NewEncoder(w).Encode(result)
    }()
    if err != nil {
		fmt.Println(err.Error())
		http.Error(w, err.Error(), 500)
	}
}
    
func (tq *tablesQuery) serve(w http.ResponseWriter, r *http.Request) {

	err := func() error {
		//st := time.Now()
		err := r.ParseForm()
		if err != nil {
			return nil
		}
		//fmt.Println(r.Method,r.Form)

		query := r.Form.Get("query")
        if query!="" {
            query = cleanQuery(query)
        }
        
        idx := -1
        if r.Form.Get("idx")!="" {
            idx = int(asInt(r.Form.Get("idx")))
        }
        
        box := quadtree.Bbox{}
        qt := quadtree.Null
        reproj:=true
        
        if r.Form.Get("minx")!="" {
        
            minx := asFloat(r.Form.Get("minx"))
            miny := asFloat(r.Form.Get("miny"))
            maxx := asFloat(r.Form.Get("maxx"))
            maxy := asFloat(r.Form.Get("maxy"))
            if false {
                fmt.Println(minx, miny, maxx, maxy)
            }
            minlonf, minlatf := quadtree.UnMercator(minx, miny)
            maxlonf, maxlatf := quadtree.UnMercator(maxx, maxy)
            if false {
                fmt.Println(minlonf, minlatf, maxlonf, maxlatf)
            }
            box.Minx, box.Miny = quadtree.ToInt(minlonf), quadtree.ToInt(minlatf)
            box.Maxx, box.Maxy = quadtree.ToInt(maxlonf), quadtree.ToInt(maxlatf)
            
            
        } else if r.Form.Get("minlon")!="" {
        
            minlon := asFloat(r.Form.Get("minlon"))
            minlat := asFloat(r.Form.Get("minlat"))
            maxlon := asFloat(r.Form.Get("maxlon"))
            maxlat := asFloat(r.Form.Get("maxlat"))
            box.Minx, box.Miny = quadtree.ToInt(minlon), quadtree.ToInt(minlat)
            box.Maxx, box.Maxy = quadtree.ToInt(maxlon), quadtree.ToInt(maxlat)
            
            
        } else if r.Form.Get("tilex") != "" {
            
            tx:=asInt(r.Form.Get("tilex"))
            ty:=asInt(r.Form.Get("tiley"))
            tz:=asInt(r.Form.Get("tilez"))
              
            qt,err:=quadtree.FromTuple(tx,ty,tz)
            if err!=nil { return err }
            box = qt.Bounds(0.0)
            //fmt.Println(tx,ty,tz,"=>",box)
        } else if r.Form.Get("quadtree") != "" {
            var err error
            if r.Form.Get("quadtree")=="ZERO" {
                qt = 0
            } else {
                qt,err = quadtree.FromString(r.Form.Get("quadtree"))
            }
            //fmt.Println(r.Form.Get("quadtree"),"=>",qt)
            if err!=nil { return err }
        } else {
        
            return errors.New("No bounds set")
        }
        
        if r.Form.Get("latlon")=="True" {
            reproj=false
        }
        
        resptype := "geojson"
        if r.Form.Get("response")!="" {
            resptype=r.Form.Get("response")
        }
        
        returnNull := false
        if r.Form.Get("returnnull")!="" {
            returnNull = (r.Form.Get("returnnull")=="true")
        }
        
        tq.lock.Lock()
        
        block,err := func() (elements.ExtendedBlock, error) {
            defer tq.lock.Unlock()
        
            qq,ok := tq.queries[idx]
            
            if (ok && (query=="" || qq.text==query)) {
                //pass
            } else {
                err = func() error {
                
                    for _,f:=range tq.queriesalt {
                        if f.text==query {
                            qq=f
                            return nil
                        }
                    }
                
                    ff,err := sqlselect.Parse(query)
                    if err!=nil { return nil }
                    qq = tabQuPair{ff,query}
                    fmt.Println("new query:",query)
                    tq.queriesalt=append(tq.queriesalt, qq)
                    return nil
                }()
                if err!=nil { return nil,err }
            }
            tq.xx++
            if box.Empty() && qt != quadtree.Null {
                //fmt.Println("pick tile??", qt)
                //box=qt.Bounds(0)
                return tq.rawtile(tq.xx,idx,qt,qq.query)
                
            }
            return tq.result(tq.xx, idx, box, qq.query)
            
        }()
        
        if err!=nil {
            return nil
        }
        if (block.Len()==0) && returnNull {
            return nil
        }

		switch resptype {
		case "geojson":

			w.Header().Set("Content-Type", "application/json")
			bl,err := geojson.MakeFeatureCollection(block, reproj)
            if err!=nil {
                panic(err.Error())
            }
			//fmt.Println("return json")
            tq.xx++
			return json.NewEncoder(w).Encode(bl)
		}
        
                
		data, err := write.WriteExtendedBlock(block, false, true, true)
		if err != nil {
			return err
		}
		//fmt.Printf("return pbf %d bytes\n", len(data))
		w.Header().Set("Content-Type", "application/pbfg")
		w.Write(data)
        
		return nil

	}()

	if err != nil {
		fmt.Println(err.Error())
		http.Error(w, err.Error(), 500)
	}
}



type deferedResult struct {
    qu   sqlselect.Tabler
    res  map[string]sqlselect.Result
    done sqlselect.Result
    //err  error
}

func (df *deferedResult) Len() int {
    if df.done==nil {
        var err error
        df.done,err = df.qu.Result(df.res)
        if err!=nil { fmt.Println(err) }
    }
    if df.done==nil { return 0 }
    return df.done.Len()
}

func (df *deferedResult) Row(i int) sqlselect.Row {
    if df.done==nil {
        var err error
        df.done,err = df.qu.Result(df.res)
        if err!=nil { fmt.Println(err) }
    }
    if df.done==nil { return nil }
    return df.done.Row(i)
}
func (df *deferedResult) Columns() []sqlselect.Rower {
    if df.done==nil {
        var err error
        df.done,err = df.qu.Result(df.res)
        if err!=nil { fmt.Println(err) }
    }
    if df.done==nil { return nil }
    return df.done.Columns()
}




func make_tables(pds packeddatastore.PackedDataStore, bb quadtree.Bbox, qt quadtree.Quadtree, spec []tableSpec) (map[string]sqlselect.Result, error) {
    
    stt:=time.Now()
    res := map[string]sqlselect.Result{}
    var err error
    sl := []string{}
    
    if (bb.Maxx-bb.Minx) > 20000000 || (bb.Maxy-bb.Miny) > 20000000 {
        return nil, errors.New(fmt.Sprintf("%s too big", bb))
    }
    
    
    
    for _,ss := range spec {
        sc:=-1
        for i,s:=range ss.cols {
            if s == "osm_id" {
                sc=i
            }
        }
        
        if ss.cols != nil {
            //res[ss.name] = filterObjs(ts,bb,ss.geomtype,ss.cols)
            if qt!=quadtree.Null {
                res[ss.name] = pds.FilterTile(qt, ss.geomtype, ss.cols,sc)
            } else {
                res[ss.name] = pds.Filter(bb, ss.geomtype, ss.cols,sc)
            }
            sl = append(sl, fmt.Sprintf("%s: %d rows",ss.name,res[ss.name].Len()))
        } else if ss.query != nil {
            res[ss.name] = &deferedResult{ss.query, res, nil}
            sl = append(sl, fmt.Sprintf("%s: defered",ss.name))
        }
        if err!=nil {
            return nil,err
        }
        //sl = append(sl, fmt.Sprintf("%s: %d rows",ss.name,res[ss.name].Len()))
    }
    
    fmt.Printf("bbox: %s => have %s: %4.1fs\n", bb, strings.Join(sl, "; "),time.Since(stt).Seconds())
    
    return res,nil
}

func geometryProgress(inc <-chan elements.ExtendedBlock, ii int) <-chan elements.ExtendedBlock {
    geometries := make(chan elements.ExtendedBlock)
    go func() {
        st:=time.Now()
        ll := ""
        li := 0
        for b := range inc {
            ll = fmt.Sprintf("%-8.1fs: %6d %0.100s %s", time.Since(st).Seconds(), b.Idx(), b, utils.MemstatsStr()) 
            li = b.Idx()
            if (li % ii)==1 {
                fmt.Println(ll)
            }
            geometries <- b
            
        }
        if (li%ii)!=1 {
            fmt.Printf(ll)
        }
        fmt.Println("close geometries")
        close(geometries)
    }()
    return geometries
}

        

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU()*2)
    cleanQueryString = regexp.MustCompile("[\n\t ]+")
        
    commonstrs := []string{}
    
    prfx := flag.String("p","","prfx")
    endstr := flag.String("e","","enddate")
    filt   := flag.String("f","planet","filter")
    stylefn := flag.String("style","extrastyle.json","stylefn")
    queriesfn:= flag.String("queries",/*"/home/james/map_data/openstreetmap/openstreetmap-carto/project-pyds.mml"*/"","queriesfn")
    commonstrings:= flag.String("commonstrings","","common strings")
    //stats := flag.Bool("stats", false, "show stats")
    outFile := flag.String("o", "", "output file")
    recalc := flag.Bool("recalc",false,"recalc qts")
    sortBlocks := flag.Bool("sort",false,"sort")
    viewsFn := flag.String("views","","viewsfn");
    
    flag.Parse()
    
    if (*prfx)=="" {
        panic("must specify file prefix")
    }
    
    var err error
    endDate := elements.Timestamp(0)
    if (*endstr)!="" {
        
        endDate,err = elements.ReadDateString(*endstr)
        if err!=nil {
             panic(err.Error())
        }
    }
    
    origfn := *prfx
    chgfns := []string{}
    qq := []quadtree.Quadtree{}
    
    if !strings.HasSuffix(*prfx, "pbf") {
    
        settings,err := locationscache.GetUpdateSettings(*prfx)
        if err!=nil {
            panic(err.Error())
        }
        var ii []locationscache.IdxItem
        ii,qq,err = locationscache.GetCacheSpecs(*prfx,settings.LocationsCache)        
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
    } else {
        _,hh,err := readfile.GetHeaderBlock(origfn)
        if err!=nil { panic(err.Error()) }
        qq = make([]quadtree.Quadtree,hh.Index.Len())
        for i,_:=range qq {
            qq[i] = hh.Index.Quadtree(i)
        }
    }
        
    
    if *commonstrings != "" {    
        sf,err:=os.Open("planet-strings.json")
        if err!=nil { panic(err.Error()) }
        err = json.NewDecoder(sf).Decode(&commonstrs)
        if err!=nil { panic(err.Error()) }
        if len(commonstrs) > 30000 {
            panic("too many strings!")
        }
        
    }
    
            
    
    
    
    var queries map[int]tabQuPair
    if *queriesfn != "" {
        queries,err = readQueriesFn(*queriesfn)
        if err!=nil {panic(err.Error()) }
    }
    
    passQt := func(q quadtree.Quadtree) bool { return true }
    
    var locTest filter.LocTest
    
    if *filt!="" {
        locTest = filter.MakeLocTest(*filt)
        fmt.Println(locTest)
        
        qs := map[quadtree.Quadtree]bool{}
        ql:=make([]quadtree.Quadtree,0,len(qq))
        for _,q:=range qq {
            //if q.Bounds(0.05).Intersects(*fbx) {
            if locTest.IntersectsQuadtree(q) {
                qs[q]=true
                ql=append(ql,q)
            }
        }
        fmt.Printf("have %d qts\n", len(qs))
        passQt = readfile.MakePassQt(qs)
        qq=ql
    }
    
    makeInChan := func() <-chan elements.ExtendedBlock {
        rr,err := readfile.ReadExtendedBlockMultiMergeQts(origfn, chgfns, 4, passQt)
        if err!=nil {
            panic(err.Error())
        }
        return readfile.CollectExtendedBlockChans(rr)
    }
    
    
    qtt:=calcqts.MakeQtTree(qq)
    qtm := map[uint32]int{}
    for i,q:=range qq {
        qtm[qtt.Find(q)]=i
    }
    
    qtAlloc := func(e elements.FullElement) quadtree.Quadtree {
        t := qtt.Find(e.Quadtree())
        return qtt.At(t).Quadtree
    }
    
    
    tagsFilter,err := geometry.ReadStyleFile(*stylefn)
    if err!=nil { panic(err.Error()) }
    //fmt.Println(stylefn,tagsFilter)
    
    
    bs:=packeddatastore.MakeBlockStore()
    dataStore := packeddatastore.MakePackedDataStore(commonstrs, qtAlloc,bs)
    
    
    fbx:=locTest.Bbox()
    geometries,err := geometry.GenerateGeometries(makeInChan, &fbx, tagsFilter, *recalc, true)
    if err!=nil { panic(err.Error())}
    
    
    if *outFile != "" {
        outBlocks := readfile.SplitExtendedBlockChans(geometries,4)
        if (*sortBlocks) {
            alloc := func(e elements.Element) int {
                qt:=e.(elements.Quadtreer).Quadtree()
                gi:=qtt.Find(qt)
                return qtm[gi]
            }
            
            makeBlock := func(i int, a int, bl elements.Block) (elements.ExtendedBlock,error) {
                qt := quadtree.Null
                if a<0 || a>= len(qq) {
                    fmt.Println("????", a, len(qq))
                } else {
                    qt = qq[a]
                }
                    
                return elements.MakeExtendedBlock(i,bl,qt,0,0,nil),nil
                
            }
            
            outBlocks, err = blocksort.SortElementsByAlloc(outBlocks,alloc,4,makeBlock,"tempfilesplit")
            if err!=nil { panic(err.Error())}
        }
        _,err = writefile.WritePbfFile(outBlocks, *outFile, false,true)
        if err!=nil { panic(err.Error())}
        return
    }
    
    for b := range geometryProgress(geometries,1273) {
        dataStore.AddBlock(b)
    }
    
    debug.FreeOSMemory()
    a,b:=bs.Stats()
    fmt.Printf("%d blobs: %5.1fmb\n", a, float64(b)/1024.0/1024.0)
    
    ql := make([]int,0,len(queries))
    for k,_:=range queries {
        ql=append(ql,k)
    }
    sort.Ints(ql)
    

    ptc := make([]string, 0, 5+len(tagsFilter))
    lnc := make([]string, 0, 5+len(tagsFilter))
    pyc := make([]string, 0, 5+len(tagsFilter))
    ptc = append(ptc, "osm_id","quadtree","way")   
    lnc = append(lnc, "osm_id","quadtree","way","z_order")
    pyc = append(pyc, "osm_id","quadtree","way","z_order","way_area")

    for k,v := range tagsFilter {
        if v.IsNode {
            ptc=append(ptc,k)
        }
        if v.IsWay {
            lnc = append(lnc,k)
            pyc = append(pyc,k)
        }
    }
    sort.Strings(ptc[3:])
    sort.Strings(lnc[4:])
    sort.Strings(pyc[5:])

    
    
        


    spec := make([]tableSpec,4)
    spec[0] = tableSpec{"planet_osm_point",geometry.Point,ptc,nil}
    spec[1] = tableSpec{"planet_osm_line",geometry.Linestring,lnc,nil}
    spec[2] = tableSpec{"planet_osm_polygon",geometry.Polygon,pyc,nil}
    
    rdq,err := sqlselect.Parse("SELECT osm_id,name,ref,admin_level,highway,railway,boundary, service,tunnel,bridge,covered,z_order, way FROM planet_osm_line WHERE highway in ( 'secondary','secondary_link','primary','primary_link', 'trunk','trunk_link','motorway','motorway_link') OR railway is not null or boundary = 'administrative' UNION SELECT osm_id,name,null as ref,admin_level,null as highway, null as railway, boundary, null as service, null as tunnel,null as bridge, 0 as z_order,way FROM planet_osm_polygon WHERE osm_id<0 and boundary='administrative'")
    if err!=nil { panic(err.Error()) }
    
    
    spec[3] = tableSpec{"planet_osm_roads",geometry.Linestring,nil, rdq}
    
    if *viewsFn != "" {
        vspec,err := readViewsFn(*viewsFn)
        if err!=nil {
            panic(err.Error())
        }
        spec = append(spec, vspec...)
    }

    tables := newTablesQuery(dataStore, spec, queries)
    
    
    http.HandleFunc("/query", tables.serve)
    http.HandleFunc("/tiles", tables.tiles)
    
    
    fmt.Println("Listening... on localhost:17831")
    panic(http.ListenAndServe(":17831", nil))

}
