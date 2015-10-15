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
    "github.com/jharris2268/osmquadtree/geometry"
	"github.com/jharris2268/osmquadtree/readfile"
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/utils"
    "github.com/jharris2268/osmquadtree/quadtree"
    "github.com/jharris2268/osmquadtree/locationscache"
	

    
	"github.com/lib/pq"
	
    "flag"
    "database/sql"
	"fmt"
	"time"
	"runtime"
	"errors"
	"strings"
    "sync"
)

type colType struct {
	col, typ string
}

var asMerc = true
var epsg = 900913

func make_point_col(cols map[string]geometry.TagTest) []colType {
	res := make([]colType, 0, len(cols)+4)
	res = append(res, colType{"osm_id", "bigint"})
	res = append(res, colType{"quadtree", "text"})
	for k, v := range cols {
		if v.IsNode {
			res = append(res, colType{k, "text"})
		}
	}
	//res=append(res,colType{"parent_highway","text"})
	return res
}

func make_line_col(cols map[string]geometry.TagTest) []colType {
	res := make([]colType, 0, len(cols)+4)
	res = append(res, colType{"osm_id", "bigint"})
	res = append(res, colType{"quadtree", "text"})
    
	for k, v := range cols {
		if v.IsWay && v.Type == "text" {
			res = append(res, colType{k, "text"})
		}
	}
	
    if _,ok := cols["min_admin_level"]; ok {
        res = append(res, colType{"min_admin_level", "text"})
        res = append(res, colType{"max_admin_level", "text"})
    }
    if _,ok := cols["bus_routes"]; ok {
        res = append(res, colType{"bus_routes", "text"})
    }

	res = append(res, colType{"z_order", "integer"})
	return res
}

func make_polygon_col(cols map[string]geometry.TagTest) []colType {
	res := make([]colType, 0, len(cols)+4)
	res = append(res, colType{"osm_id", "bigint"})
	res = append(res, colType{"quadtree", "text"})
	for k, v := range cols {

		if v.IsWay && v.Type == "text" {
			res = append(res, colType{k, "text"})
		}
	}

	res = append(res, colType{"z_order", "integer"})
	res = append(res, colType{"way_area", "real"})
	return res
}

func joinCols(cols []colType) string {
	ans := make([]string, len(cols))
	for i, c := range cols {
		ans[i] = fmt.Sprintf("\"%s\" %s", c.col, c.typ)
	}
	return strings.Join(ans, ", ")
}

func exec_queries_list(db *sql.DB, queries []queryPair) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
            
			errp, ok := err.(*pq.Error)
			if !ok {
				println("not a pq error", err.Error())
			} else {
				fmt.Printf("%s %s %s %s\n", errp.Code, errp.Message, errp.Hint, errp.Position)
			}
			tx.Rollback()
			return
		}
		err = tx.Commit()
	}()

	tt := time.Now()
	for i, q := range queries {
		st := time.Now()
        if len(q.vals)>0 {
            qn := q.vals[0].(string)
                
            fmt.Printf("%-2d: %-30s", i, qn)
		}
        
        _, err = tx.Exec(q.stmt)
        

		if err != nil {
			fmt.Printf(" FAILED[%d] %s\n", len(q.stmt), q.stmt)
			return err
		}
		fmt.Printf("%8.1fs\n", time.Since(st).Seconds())
	}
	fmt.Printf("added %d queries in %8.1fs\n", len(queries), time.Since(tt).Seconds())
	return err
}

func prepare(db *sql.DB, prfx string, cols map[string]geometry.TagTest) (map[string][]colType, error) {

	stmts := make([]queryPair, 0, 11)

	stmts = append(stmts, queryPair{fmt.Sprintf("drop table if exists %s_point cascade", prfx), []interface{}{"drop point"}})
	stmts = append(stmts, queryPair{fmt.Sprintf("drop table if exists %s_line cascade", prfx), []interface{}{"drop line"}})
	stmts = append(stmts, queryPair{fmt.Sprintf("drop table if exists %s_polygon cascade", prfx), []interface{}{"drop polygon"}})
    stmts = append(stmts, queryPair{fmt.Sprintf("drop table if exists %s_roads cascade", prfx), []interface{}{"drop roads"}})
    
	stmts = append(stmts, queryPair{fmt.Sprintf("delete from geometry_columns where f_table_name like '%s%%'", prfx), []interface{}{"delete geometry_columns"}})

	ptCol := make_point_col(cols)
	lnCol := make_line_col(cols)
	pyCol := make_polygon_col(cols)

	stmts = append(stmts, queryPair{fmt.Sprintf("create table %s_point ( %s )", prfx, joinCols(ptCol)), []interface{}{"add point"}})
	stmts = append(stmts, queryPair{fmt.Sprintf("create table %s_line ( %s )", prfx, joinCols(lnCol)), []interface{}{"add line"}})
	stmts = append(stmts, queryPair{fmt.Sprintf("create table %s_polygon( %s )", prfx, joinCols(pyCol)), []interface{}{"add polygon"}})

	stmts = append(stmts, queryPair{fmt.Sprintf("select AddGeometryColumn('%s_point', 'way', %d, 'POINT', 2)", prfx, epsg), []interface{}{"add point way"}})
	stmts = append(stmts, queryPair{fmt.Sprintf("select AddGeometryColumn('%s_line', 'way', %d, 'LINESTRING', 2)", prfx, epsg), []interface{}{"add line way"}})
	stmts = append(stmts, queryPair{fmt.Sprintf("select AddGeometryColumn('%s_polygon', 'way', %d, 'GEOMETRY', 2)", prfx, epsg), []interface{}{"add polygon way"}})

	err := exec_queries_list(db, stmts)

	return map[string][]colType{"point": ptCol, "line": lnCol, "polygon": pyCol}, err
}

func prepcols(colnames []string) (string, string) {
	cols := make([]string, len(colnames))
	vals := make([]string, len(colnames))
	for i, c := range colnames {
		cols[i] = fmt.Sprintf("\"%s\"", c)
		vals[i] = fmt.Sprintf("$%d", i+1)
		if c == "way" {
			//vals[i] = fmt.Sprintf("ST_GeomFromText($%d, 4326)", i+1)
			vals[i] = fmt.Sprintf("ST_GeomFromWkb($%d, %d)", i+1, epsg)
		}
	}
	return strings.Join(cols, ", "), strings.Join(vals, ", ")
}

func passKey(colSpec map[string]geometry.TagTest, tab string, key string) bool {
	cs, ok := colSpec[key]
	if !ok {
		return false
	}
	if tab == "point" {
		return cs.IsNode
	}
	if tab == "polygon" {
		switch key {
		case "min_admin_level", "max_admin_level":
			return false
		}
	}
	return true
}

type copyPair struct {
	stmt *sql.Stmt
	tx   *sql.Tx
	spec map[string]int
}

func prepareCopyStmt(tx *sql.Tx, table string, spec []colType) (copyPair, error) {
	cc := make([]string, 0, len(spec)+1)
	cols := map[string]int{}
	for i, sp := range spec {
		cc = append(cc, sp.col)
		cols[sp.col] = i
	}
	cc = append(cc, "way")
	cols["way"] = len(cols)
	stmt, err := tx.Prepare(pq.CopyIn(table, cc...))
	return copyPair{stmt, tx, cols}, err
}

//var errsf,_ = os.Create("geomswritten.txt")

func prepareCopyRow(specs map[string]copyPair, ele_in elements.Element) (string, []interface{}, error) {

	ele, err := geometry.ExtractGeometry(ele_in)

	if err != nil {
        fmt.Println(ele_in,"not a geometry")
		return "", nil, err
	}
	
    if !ele.IsValid() {
        fmt.Println(ele.GeometryType(),ele.Id() & 0xffffffffffff,ele.AsWkt(true),"not a valid geometry")
        return "",nil,nil
    }
    
	tab := ""

	switch ele.GeometryType() {
        case 1:
            tab = "point"
        case 2:
            tab = "line"
        case 3, 7:
            tab = "polygon"
        default:
            return "", nil, errors.New("unexpected geometry type")
    }

	sp := specs[tab].spec
	vals := make([]interface{}, len(sp))

	oid := ele.Id() & 0xffffffffff
	if (ele.Id() >> 59) == 2 {
		vals[sp["osm_id"]] = oid * -1
	} else {
		vals[sp["osm_id"]] = oid
	}
    
    qi,ok := sp["quadtree"]
	if ok {
        vals[qi] = ele.Quadtree().String()
    }

	for i := 0; i < ele.Tags().Len(); i++ {
		k := ele.Tags().Key(i)
		ii, ok := sp[k]
		if ok {
			vals[ii] = ele.Tags().Value(i)
		}
	}

	switch tab {
        case "line", "polygon":
            zo,ok := ele.(interface{ZOrder() int64})
            
            
            if ok {
                vi,ok := sp["z_order"]
                if ok {
                    vals[vi] = zo.ZOrder()
                }
            }
    }
        
    switch tab {
        case "polygon":
            ar,ok := ele.(interface{Area() float64})
            if ok {
                vi,ok := sp["way_area"]
                if ok {
                    vals[vi] = ar.Area()
                }
            }
	}
	
    
	ii, ok := sp["way"]
	if !ok {
		return "", nil, errors.New("wtf")
	}
	vals[ii] = fmt.Sprintf("SRID=%d;%s", epsg, ele.AsWkt(asMerc))
	
    if strings.Contains(vals[ii].(string), "Inf") {
		println("skip", ele.Id(), vals[ii].(string))
		return "", nil, nil
	}
	

	return tab, vals, nil
}


type queryPair struct {
	stmt string
	vals []interface{}
}


func insertCopy(spec map[string]copyPair, ele elements.Element) (queryPair, error) {
	tab, vals, err := prepareCopyRow(spec, ele)
	if err != nil || vals == nil {
		return queryPair{}, err
	}

	//_,err = tx.Exec(st, vals...)
	if err != nil {
		fmt.Printf("%s %q\n", tab, vals)
		return queryPair{}, err
	}
	return queryPair{tab, vals}, nil
}


func insertCopyBlock(outc chan<- queryPair, spec map[string]copyPair, block elements.Block) error {

	var err error
	var qp queryPair
	for i := 0; i < block.Len(); i++ {
		ele := block.Element(i)
		//err=insert(tx,prfx,colSpec,obj)
		qp, err = insertCopy(spec, ele)
		if err != nil {
			return err
		}
        if qp.stmt != "" {
			outc <- qp
		}
	}
	return err
}

var roads_stmt = `CREATE TABLE %s_roads AS
    SELECT osm_id,quadtree,name,ref,admin_level,highway,railway,boundary,
            service,tunnel,bridge,z_order,covered, way
        FROM %s_line
        WHERE highway in (
            'secondary','secondary_link','primary','primary_link',
            'trunk','trunk_link','motorway','motorway_link')
        OR railway is not null or boundary = 'administrative'
    UNION SELECT osm_id,quadtree,name,null as ref,admin_level,null as highway, 
            null as railway, boundary, null as service,
            null as tunnel,null as bridge, 0  as z_order,null as covered,
            exteriorring(st_geometryn(way,generate_series(1,st_numgeometries(way)))) as way
        FROM %s_polygon WHERE
            osm_id<0 and boundary='administrative' and geometrytype(way)!='POLYGON'
    UNION SELECT osm_id,quadtree,name,null as ref, admin_level,null as highway,
            null as railway, boundary, null as service,
            null as tunnel,null as bridge, 0  as z_order,null as covered,
            exteriorring(way) as way
        FROM %s_polygon WHERE
            osm_id<0 and boundary='administrative' and geometrytype(way)='POLYGON'`

func finish(db *sql.DB, prfx string) error {

	stmts := make([]queryPair, 0, 10)
	println("call finish")
	stmts = append(stmts, queryPair{fmt.Sprintf(roads_stmt, prfx, prfx, prfx, prfx), []interface{}{"create roads view"}})
    
	
	for _, p := range []string{"point", "line", "polygon", "roads"} {

	
		q := fmt.Sprintf("create index %s_%s_pkey on %s_%s using btree ( osm_id )", prfx, p, prfx, p)
		stmts = append(stmts,  queryPair{q, []interface{}{"create id index " + p}})

		q = fmt.Sprintf("create index %s_%s_way on %s_%s using gist ( way )", prfx, p, prfx, p)
		stmts = append(stmts,  queryPair{q, []interface{}{"create geo index " + p}})

	}
	stmts = append(stmts,  queryPair{"create or replace function makeinteger(text) returns integer as $$ select case when $1~E'^-?[0123456789]+$' then $1::integer else 0 end;$$ language sql;", []interface{}{"createfunc"}})
	
	return exec_queries_list(db, stmts)
}

func InsertAll(dbname string, prfx string, cols map[string]geometry.TagTest, inc <-chan elements.ExtendedBlock, vac bool) error {

	connstr := "host=/var/run/postgresql sslmode=disable dbname=" + dbname
	
	db, err := sql.Open("postgres", connstr)
	if err != nil {
		return err
	}

	defer db.Close()
	err = db.Ping()
	if err != nil {
		println("ping failed ", connstr)
		return err
	}

	colspecs, err := prepare(db, prfx, cols)
	t1 := time.Now()

	if err != nil {
		return err
	}

	err = func() error {
		fix := func(cc *copyPair) {
			//errsf.Sync()
			cc.stmt.Exec()
			if err != nil {
				errp, ok := err.(*pq.Error)
				if !ok {
					println("not a pq error")
				} else {
					fmt.Printf("%s %s %s %s\n", errp.Code, errp.Message, errp.Hint, errp.Position)
				}
				cc.tx.Rollback()
				return
			}
			err = cc.tx.Commit()
		}

		copystmts := map[string]copyPair{}
		for k, v := range colspecs {
			tx, err := db.Begin()
			if err != nil {
				return err
			}

			st, err := prepareCopyStmt(tx, fmt.Sprintf("%s_%s", prfx, k), v)
			
            fmt.Println(k, len(v), len(st.spec))
			
            if err != nil {
				return err
			}
			copystmts[k] = st
			defer fix(&st)
		}


        qchan := make(chan queryPair)
        
		        
		go func() {
            wg:=sync.WaitGroup{}
            wg.Add(4)
            
            for i:=0; i < 4; i++ {
                go func() {
                    for bl := range inc {
                        
                        err := insertCopyBlock(qchan, copystmts, bl)
                        if err != nil {
                            fmt.Println("insertCopyBlock", bl)
                            panic(err.Error())
                        }
                    }
                    wg.Done()
                }()
            }
            wg.Wait()
            close(qchan)
            
		}()
        
        
        for qp := range qchan {
            _, err := copystmts[qp.stmt].stmt.Exec(qp.vals...)
        
            if err != nil {
                fmt.Println(qp.stmt)
                fmt.Println(qp.vals)
                fmt.Println("copy statment exec", err.Error())
                //return err
                panic(err.Error())
            }
        }
		
		return nil
	}()
	if err != nil {
		return nil
	}
	fmt.Printf("Insert: %8.1fs\n", time.Since(t1).Seconds())
	err = finish(db, prfx)
	if err != nil {
		return nil
	}
    if vac {
        ft := time.Now()
        _, err = db.Exec("VACUUM ANALYZE")
        fmt.Printf("VACUUM ANALYZE: %8.1fs\n", time.Since(ft).Seconds())
    }
	return err
}

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
    
    //inpr := "/home/james/map_data/repl_diffs/british-isles/" //643.osc.gz"
    prfx    :=flag.String("p","planet/","data prefix")
    endstr  :=flag.String("e","","endstr")
    filter  :=flag.String("f","planet","filter")
    
    stylefn :=flag.String("s","extrastyle.json","stylefn")
    recalc  :=flag.Bool("q",false,"recalc qts")
    
    dbname  :=flag.String("d", "gis", "dbname")
    tablep  :=flag.String("t", "planet_osm", "table prefix")
    vac     :=flag.Bool("v", false, "call vacuum analyze")
    
    flag.Parse()
    endDate:=elements.Timestamp(0)
    var err error
    if *endstr != "" {
        endDate,err = elements.ReadDateString(*endstr)
        if err!=nil {
             panic(err.Error())
        }
    }
    origfn := *prfx
    chgfns := []string{}
    qq     := []quadtree.Quadtree{}
    
    if strings.HasSuffix(*prfx, ".pbf") {
        _,idx,err := readfile.GetHeaderBlock(*prfx)
        if err!=nil {
            panic(err.Error())
        }
        
        qq = make([]quadtree.Quadtree,idx.Index.Len())
        for i,_ := range qq {
            qq[i] = idx.Index.Quadtree(i)
        }
    } else {
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
                if i.Timestamp >= endDate {
                    break
                }
            }
            if len(chgfns) > 0 {
                fmt.Printf("origfn:%s, %d changes [%s=>%s]\n",origfn, len(chgfns), chgfns[0], chgfns[len(chgfns)-1])
            }
        }
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
    
    
    tagsFilter,err := geometry.ReadStyleFile(*stylefn)
    if err!=nil { panic(err.Error()) }
    
    geometries,err := geometry.GenerateGeometries(makeInChan, fbx, tagsFilter,*recalc,false)
    if err!=nil {
        panic(err.Error())
    }
    
    err = InsertAll(*dbname, *tablep, tagsFilter, geometryProgress(geometries,1273), *vac)
    if err!=nil {
        panic(err.Error())
    }
    
}

