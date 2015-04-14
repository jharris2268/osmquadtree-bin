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
    //"github.com/jharris2268/osmquadtree/blocksort"
    "github.com/jharris2268/osmquadtree/write"
    "github.com/jharris2268/osmquadtree/geojson"
    "github.com/jharris2268/osmquadtree/sqlselect"
    "github.com/jharris2268/osmquadtree/writefile"
    
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
)

var cleanQueryString *regexp.Regexp



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




type tableSpec struct {
    name string
    geomtype geometry.GeometryType
    cols []string
    query sqlselect.Tabler
}

type tablesQueryResult struct {
    bbox   quadtree.Bbox
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
}

func newTablesQuery(pds packeddatastore.PackedDataStore, spec []tableSpec, queries map[int]tabQuPair) *tablesQuery {
    return &tablesQuery{pds,spec,queries,make([]tabQuPair,0,16),map[int]tablesQueryResult{},-1,0,0}
}

func (tq *tablesQuery) tables(bb quadtree.Bbox) (map[string]sqlselect.Result,error) {
    for _,v:=range tq.prev {
        if v.bbox == bb {
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
    
    
    nr,err := make_tables(tq.pds,bb,tq.spec)
    
    
    if err!=nil {
        return nil,err
    }
    
    tq.prev[tq.mx] = tablesQueryResult{bb,nr}
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
    if err!=nil { return nil,err }
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
    ble := elements.MakeExtendedBlock(idx,bl,quadtree.Null,0,0,elements.MakeTags([]string{"idx","query","tags"},[]string{fmt.Sprintf("%d",qui),qu.String(),ts}))
    return ble,nil
}

func cleanQuery(qu string) string {
    a := cleanQueryString.ReplaceAllString(qu," ")
    b := strings.TrimSpace(a)
    //return strings.ToLower(b)
    return b
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
            fmt.Println(tx,ty,tz,"=>",box)
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
            if err!=nil { return nil }
        }
        
        block,err := tq.result(tq.xx, idx, box, qq.query)
        
        if err!=nil { return err }
        
        

		switch resptype {
		case "geojson":

			w.Header().Set("Content-Type", "application/json")
			bl := geojson.MakeFeatureCollection(block, reproj)
			//fmt.Println("return json")
            tq.xx++
			return json.NewEncoder(w).Encode(bl)
		}
        
                
		data, err := write.WriteExtendedBlock(block, false, true)
		if err != nil {
			return err
		}
		//fmt.Printf("return pbf %d bytes\n", len(data))
		w.Header().Set("Content-Type", "application/pbfg")
		w.Write(data)
        tq.xx++
		return nil

	}()

	if err != nil {
		fmt.Println(err.Error())
		http.Error(w, err.Error(), 500)
	}
}

func make_tables(pds packeddatastore.PackedDataStore, bb quadtree.Bbox, spec []tableSpec) (map[string]sqlselect.Result, error) {
    
    stt:=time.Now()
    res := map[string]sqlselect.Result{}
    var err error
    sl := []string{}
    
    if (bb.Maxx-bb.Minx) > 20000000 || (bb.Maxy-bb.Miny) > 20000000 {
        return nil, errors.New(fmt.Sprintf("%s too big", bb))
    }
    
    for _,ss := range spec {
        if ss.cols != nil {
            //res[ss.name] = filterObjs(ts,bb,ss.geomtype,ss.cols)
            res[ss.name] = pds.Filter(bb, ss.geomtype,ss.cols)
        } else if ss.query != nil {
            res[ss.name],err = ss.query.Result(res)
        }
        if err!=nil {
            return nil,err
        }
        sl = append(sl, fmt.Sprintf("%s: %d rows",ss.name,res[ss.name].Len()))
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
    filter := flag.String("f","planet","filter")
    stylefn := flag.String("style","extrastyle.json","stylefn")
    queriesfn:= flag.String("queries",/*"/home/james/map_data/openstreetmap/openstreetmap-carto/project-pyds.mml"*/"","queriesfn")
    commonstrings:= flag.String("commonstrings","","common strings")
    //stats := flag.Bool("stats", false, "show stats")
    outFile := flag.String("o", "", "output file")
    
    
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
    
    fbx := quadtree.PlanetBbox()
    if *filter!="" {
        fbx = readBbox(*filter)
        fmt.Println(fbx)
        qs := map[quadtree.Quadtree]bool{}
        ql:=make([]quadtree.Quadtree,0,len(qq))
        for _,q:=range qq {
            if q.Bounds(0.05).Intersects(*fbx) {
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
    
    
    geometries,err := geometry.GenerateGeometries(makeInChan, fbx, tagsFilter, true, true)
    if err!=nil { panic(err.Error())}
    if *outFile != "" {
        _,err = writefile.WritePbfFile(readfile.SplitExtendedBlockChans(geometries,4), *outFile, false)
        if err!=nil { panic(err.Error())}
        return;
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




    tables := newTablesQuery(dataStore, spec, queries)
    
    
    http.HandleFunc("/query", tables.serve)
    
    fmt.Println("Listening... on localhost:17831")
    panic(http.ListenAndServe(":17831", nil))

}
