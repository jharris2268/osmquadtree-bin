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
    //"github.com/jharris2268/osmquadtree/write"
    //"github.com/jharris2268/osmquadtree/geojson"
    //"github.com/jharris2268/osmquadtree/sqlselect"
    "github.com/jharris2268/osmquadtree/writefile"
    "github.com/jharris2268/osmquadtree/blocksort"
    
    //"github.com/jharris2268/osmquadtree/packeddatastore"
    
    "runtime"
    //"runtime/debug"
    "strings"
    "encoding/json"
    //"errors"
    "os"
    "fmt"
    //"unsafe"
    "time"
    "sort"
    "regexp"
    "math"
    "strconv"
    //"net/http"
    "flag"
    //"sync"
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



func packInt(i uint64) string {
	r := make([]byte, 10)
	p := utils.WriteUvarint(r, 0, i)
	return string(r[:p])
}
func tagIntVal(v int64) string {
    return packInt(utils.Zigzag(v))
}

func tagFloatVal(v float64) string {
    return packInt(math.Float64bits(v))
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
type tagtype int
const (
    nulltag tagtype = iota
    strtag
    inttag
    floattag
)
type tag interface {
    Ok()        bool
    Pack()      (string,string)
    Key()       string 
    Value()     interface{}
    Type()      tagtype
    
}

type nullTag struct {
    key string
}
func (tg *nullTag) Ok() bool { return false }
func (tg *nullTag) Pack() (string,string) { return "$"+tg.key,"" }
func (tg *nullTag) Key() string { return tg.key }
func (tg *nullTag) Value() interface{} { return nil }
func (tg *nullTag) Type() tagtype { return nulltag }

type strTag struct {
    key string
    val string
}
func (tg *strTag) Ok() bool { return true }
func (tg *strTag) Pack() (string,string) { return tg.key,tg.val }
func (tg *strTag) Key() string { return tg.key }
func (tg *strTag) Value() interface{} { return tg.val }
func (tg *strTag) Type() tagtype { return strtag }

type intTag struct {
    key string
    val int64
}
func (tg *intTag) Ok() bool { return true }
func (tg *intTag) Pack() (string,string) { return "^"+tg.key,tagIntVal(tg.val) }
func (tg *intTag) Key() string { return tg.key }
func (tg *intTag) Value() interface{} { return tg.val }
func (tg *intTag) Type() tagtype { return inttag }

type floatTag struct {
    key string
    val float64
}
func (tg *floatTag) Ok() bool { return true }
func (tg *floatTag) Pack() (string,string) { return "%"+tg.key,tagFloatVal(tg.val) }
func (tg *floatTag) Key() string { return tg.key }
func (tg *floatTag) Value() interface{} { return tg.val }
func (tg *floatTag) Type() tagtype { return floattag }

type renameTag struct {
    orig tag
    newk string
}
func (tg *renameTag) Ok() bool { return tg.orig.Ok() }
func (tg *renameTag) Pack() (string,string) {
    a,b := tg.orig.Pack()
    if len(a)==0 { return a,b }
    switch a[:1] {
        case "$","%","^": return a[:1]+tg.newk, b
    }
    return tg.newk,b
}
    
func (tg *renameTag) Key() string { return tg.newk }
func (tg *renameTag) Value() interface{} { return tg.orig.Value() }
func (tg *renameTag) Type() tagtype { return tg.orig.Type() }



type tagSlice []tag
func (ts tagSlice) Pack(includeNulls bool) elements.Tags {
    kk,vv := make([]string, 0, ts.Len()), make([]string, 0, ts.Len())
    for _,t:=range ts {
        if includeNulls || t.Ok() {
            k,v:=t.Pack()
            kk=append(kk,k)
            vv=append(vv,v)
        }
    }
    return elements.MakeTags(kk,vv)
}
func (ts tagSlice) Len() int { return len(ts) }

type feature interface {
    Table()   string
    Len()   int
    Feature(i int) string
    Tags() tagSlice
}
    

type featureImpl struct {
    table   string
    key     string
    value   string
    tags    tagSlice
}

func (fi *featureImpl) Table() string { return fi.table }
func (fi *featureImpl) Len() int { return 1 }
func (fi *featureImpl) Feature(i int) string {
    if i>0 { return "" }
    if fi.key == "" { return "" }
    if fi.value == "" { return fi.key }
    return fi.key+"_"+fi.value
}

func (fi *featureImpl) Tags() tagSlice {
    rs := tagSlice{}
    f:=fi.Feature(0)
    if f!="" {
        rs=append(rs, &strTag{"feature",f})
    }
    if fi.tags != nil {
        rs=append(rs, fi.tags...)
    }
    return rs
}
type prioFeature struct {
    tab    string
    fi     feature
    prio   int64
}

func (pf *prioFeature) Table() string { return pf.tab }
func (pf *prioFeature) Len() int { return pf.fi.Len() }
func (pf *prioFeature) Feature(i int) string {
    return pf.fi.Feature(i)
}
func (pf *prioFeature) Tags() tagSlice {
    return append(pf.fi.Tags(), &intTag{"prio",pf.prio})
}

type groupFeature struct {
    table string
    feats featureSlice
}
func (gf *groupFeature) Table() string { return gf.table }
func (gf *groupFeature) Len() int { return len(gf.feats) }
func (gf *groupFeature) Feature(i int) string { return gf.feats[i].Feature(0) }
func (gf *groupFeature) Tags() tagSlice {
    ans := make(tagSlice, 0, 2*gf.Len())
    for _,f:=range gf.feats {
        ans=append(ans, f.Tags()...)
    }
    return ans
}
type featureSlice []feature
func (fs featureSlice) Len() int { return len(fs) }
func (fs featureSlice) Swap(i,j int) { fs[i],fs[j]=fs[j],fs[i] }
func (fs featureSlice) Less(i,j int) bool {
    if fs[i].Table()==fs[j].Table() {
        return fs[i].Feature(0) < fs[j].Feature(0)
    }
    return fs[i].Table() < fs[j].Table()
}
func (fs featureSlice) Sort() { sort.Sort(fs) }


type featureSpec interface {
    Handle(geometry.Geometry ) feature
}    

type tableSpec interface {
    Name() string
    Tags(feature, geometry.Geometry) elements.Tags
}

type colSpec interface {
    Proc(geometry.Geometry) tag
}

type passcol struct {
    key string
}
func (pf *passcol) Proc(geom geometry.Geometry) tag {
    tags:=geom.Tags().(geometry.TagsEditable)
    if tags.Has(pf.key) {
        return &strTag{pf.key, tags.Get(pf.key)}
    }
    return &nullTag{pf.key}
}

type renamecol struct {
    orig colSpec
    newkey  string
}
func (pf *renamecol) Proc(geom geometry.Geometry) tag {
    tg := pf.orig.Proc(geom)
    return &renameTag{tg, pf.newkey}
}

type cutsuffix struct {
    orig  colSpec
    suff  string
}

func (cs *cutsuffix) Proc(geom geometry.Geometry) tag {
    tg := cs.orig.Proc(geom)
    if tg.Type()!=strtag { return &nullTag{tg.Key() } }
    v:=tg.Value().(string)
    if strings.HasSuffix(v, cs.suff) {
        return &strTag{tg.Key(), strings.TrimSuffix(v,cs.suff)}
    }
    return tg
}

type replval struct {
    orig    colSpec
    repls   map[string]string
}
func (rv *replval) Proc(geom geometry.Geometry) tag {
    tg := rv.orig.Proc(geom)
    k := tg.Key()
    if tg.Type()!=strtag { return &nullTag{k} }
    v:=tg.Value().(string)
    
    rr, ok := rv.repls[v]
    if ok {
        if rr=="" {
            return tg
        } else {
            return &strTag{k, rr}
        }
    }
    rr,ok = rv.repls[""]
    if ok {
        return &strTag{k, rr}
    }
    return &nullTag{k}
}

type yesno struct {
    orig colSpec
}

func (yn *yesno) Proc(geom geometry.Geometry) tag {
    tg := yn.orig.Proc(geom)
    k := tg.Key()
    if tg.Type()!=strtag { return &nullTag{k} }
    v := tg.Value().(string)
    switch v {
        case "no","0","false": return &strTag{k,"no"}
    }
    if v!="" {
        return &strTag{k,"yes"}
    }
    return &nullTag{k} 
}


type hassuffix struct {
    orig    colSpec
    suff    string
}

func (hs *hassuffix) Proc(geom geometry.Geometry) tag {
    tg := hs.orig.Proc(geom)
    k := tg.Key()
    if tg.Type()!=strtag { return &nullTag{k} }
    v := tg.Value().(string)
    
    if strings.HasSuffix(v, hs.suff) {
        return &strTag{k,"yes"}
    }
    return &strTag{k,"no"}
}

type colSpecMany interface{
    ProcMany(geom geometry.Geometry) tagSlice
}

type splitcol struct {
    orig colSpec
    split   string
    numvals string
    maxlen  string
}

func (sc *splitcol) ProcMany(geom geometry.Geometry) tagSlice {
    tg := sc.orig.Proc(geom)
    k := tg.Key()
    if tg.Type()!=strtag { return nil }
    v := tg.Value().(string)
    
    fs := strings.Split(v, sc.split)
    res := make(tagSlice, 0, len(fs)+2)
    for _,f := range fs {
        res=append(res, &strTag{k, f})
    }
    if sc.numvals != "" {
        res=append(res, &intTag{sc.numvals, int64(len(fs))})
    }
    if sc.maxlen != "" {
        mxl:=0
        for _,f:=range fs {
            if len(f)>mxl { mxl=len(f) }
        }
        res=append(res, &intTag{sc.maxlen, int64(mxl)})
    }
    return res
}

type joincol struct {
    l, r colSpec
}
func (jc *joincol) Proc(geom geometry.Geometry) tag {
    l := jc.l.Proc(geom)
    r := jc.r.Proc(geom)
    if r.Type() == nulltag { return l }
    if l.Type() == nulltag {
        return &strTag{l.Key(), r.Value().(string)}
    }
    if l.Type()!= strtag || r.Type()!=strtag { return &nullTag{l.Key()} }
    
    nv := l.Value().(string)+" "+r.Value().(string)
    return &strTag{l.Key(), nv}
}

type isval struct {
    col colSpec
    val string
}
func (iv *isval) Proc(geom geometry.Geometry) tag {
    v := iv.col.Proc(geom)
    if v.Type()==strtag && v.Value().(string)==iv.val {
        return &strTag{v.Key(), "yes"}
    }
    return &strTag{v.Key(), "no"}
}

type haseither struct {
    key  string
    cols []colSpec
    
}

func (he *haseither) Proc(geom geometry.Geometry) tag {
    for _,cc := range he.cols {
        tg := cc.Proc(geom)
        if tg.Type()==strtag && tg.Value().(string) == "yes" {
            return &strTag{he.key, "yes"}
        }
    }
    return &strTag{he.key, "no"}
}

type wayArea struct{}
func (wa *wayArea) Proc(geom geometry.Geometry) tag {
    aa,ok := geom.(interface{ Area() float64 })
    if ok {
        return &floatTag{"way_area", aa.Area()}
    }
    return &nullTag{"way_area"}
}

type asint struct {
    col colSpec
}
func (ai *asint) Proc(geom geometry.Geometry) tag {
    v := ai.col.Proc(geom)
    if v.Type() == inttag { return v }
    if v.Type() == nulltag { return v }
    if v.Type() == floattag {
        return &intTag{v.Key(), int64(v.Value().(float64))}
    }
    vs := v.Value().(string)
    i,_,err := utils.ParseStringInt(vs)
    if err!=nil {
        return &intTag{v.Key(), i}
    }
    return &nullTag{v.Key()}
}

type tableSpecImpl struct {
    name string
    cols []colSpec
    colManys []colSpecMany
}

func (ts *tableSpecImpl) Name() string {
    return ts.name
}

func (ts *tableSpecImpl) Tags(feat feature, geom geometry.Geometry) elements.Tags {
    tgs := make(tagSlice, 0, len(ts.cols)+2)
    
    tgs = append(tgs, &strTag{"table",ts.name})
    
    tgs = append(tgs, feat.Tags()...)
    
    for _,cl := range ts.cols {
        tgs = append(tgs, cl.Proc(geom))
    }
    for _,cl := range ts.colManys {
        tgs = append(tgs, cl.ProcMany(geom)...)
    }
    
    
    return tgs.Pack(false)
}


type passVal struct {
    col   colSpec
    
    point string
    line  string
    poly  string
}

func (pv *passVal) table(gt geometry.GeometryType) string {
    switch gt {
        case geometry.Point: return pv.point
        case geometry.Linestring: return pv.line
        case geometry.Polygon, geometry.MultiPolygon: return pv.poly
    }
    return ""
}

func (pv *passVal) Handle(geom geometry.Geometry) feature {
    
    
    tab := pv.table(geom.GeometryType())
    if (tab == "") { return nil }
    
    tg := pv.col.Proc(geom)
    if !tg.Ok() { return nil }
    
    return &featureImpl{tab, tg.Key(), tg.Value().(string),nil}
}

type checkPrio struct {
    spec     featureSpec 
    prios    map[string]int64
    prioline string
}

func (pv *checkPrio) Handle(geom geometry.Geometry) feature { 
    
    
    fs := pv.spec.Handle(geom)
    if fs==nil { return nil }
    
    
    if geom.GeometryType() == geometry.Linestring {
        if _,ok:= pv.prios[fs.Feature(0)]; ok {
            
            return &prioFeature{pv.prioline,  fs, pv.prios[fs.Feature(0)]}
        }
    }
    
    return fs
}


type isBuilding struct {
    tab string
}

func (ib *isBuilding) Handle(geom geometry.Geometry) feature {
    switch (geom.GeometryType()) {
        case geometry.Polygon, geometry.MultiPolygon: //continue
        default: return nil
    }
    tgs := geom.Tags().(geometry.TagsEditable)
    if !tgs.Has("building") { return nil }
    switch tgs.Get("building") {
        case "no", "0", "false": return nil
    }
    return &featureImpl{ib.tab, "", "", tagSlice{&strTag{"is_building","yes"}}}
}


type hasKey struct {
    newk string
    fs featureSpec
    
}


func (hk *hasKey) Handle(geom geometry.Geometry) feature {
    hh := hk.fs.Handle(geom)
    if hh ==nil {
        return nil
    }
    return &featureImpl{hh.Table(), hk.newk, "", nil }
    
}


var feat_tags []featureSpec
var table_specs map[string]tableSpec
//var highway_prios map[string]int64
/*
featkeys = {
    'shop': fix_shop,
    'amenity': passf,
    'natural': passf,
    'barrier': passf,
    'power': passf,
    'aerialway': passf,
    'route': passf,
    'man_made': passf,
    'leisure': passf,
    'historic': passf,
    'aeroway': passf,
    'place': fix_place,
    #'wetland': fix_wetland,
    'railway': fix_railway,
    'landuse': passf,
    'tourism': passf,
    'waterway': passf,
    'highway': passf,
    'admin_boundary': has_admin_level,
    'military': passf,
    'boundary': passf,
}

common_props = [
('osm_id',passf),
('quadtree',passf),
('ref',  passf),
('name', add_ele_to_name),
]


line_props = common_props + [
('addr_interpolation', FixKey('addr:interpolation')),
#('admin_level',FixKey('min_admin_level')),
('tunnel',yesNo),
('bridge',yesNo),
('intermittent',yesNo),
]

as_list = lambda k, props: props[k].split(";") if k in props else None

split_refs = lambda k,props: props['ref'].replace(";","\n") if 'ref' in props else None
refs_height = lambda k,props: len(props['ref'].split(";")) if 'ref' in props else None
refs_width = lambda k,props: max(map(len,props['ref'].split(";"))) if 'ref' in props else None

highway_props = line_props + [
('bridge', yesNo),
('tunnel', yesNo),
('access', Repl('access',[(['destination'],'destination'),(['no','private'],'no')])),
('int_surface', Repl('surface',[(surface1,'unpaved'),(surface2,'paved')])),
('service', Repl('service',[(['parking_aisle', 'drive-through', 'driveway'], 'INT-minor'),(None,'INT-normal')])),
('bicycle', passf),
('construction', passf),
('int_construction_minor',Repl('construction',[(['service', 'footway', 'cycleway', 'bridleway', 'path', 'track'], 'yes'), (None,'no')])),
('horse', passf),
('oneway', fix_oneway),
('link', lambda l,p: 'yes' if ('highway' in p and p['highway'].endswith('_link')) else 'no'),
('tracktype', passf),
('layernotnull', fix_layer),
('cycle_routes', as_list),
('bus_routes', as_list),
('refs', split_refs),
('height', refs_height),
('width', refs_width),
('covered', yesNo),
]


other_props = common_props + [
('religion', Repl('religion',[(['christian'],'christian'),(['jewish'],'jewish'),(None,'INT-generic')])),
('denomination', passf),
('operator', passf), ('access', passf),
('addr_housenumber', FixKey('addr:housenumber')), ('addr_housename', FixKey('addr:housename')),
('is_wind', lambda k,p: 'yes' if (('generator:source' in p and p['generator:source']=='wind') or ('power_source' in p and p['power_source']=='wind')) else 'no'),
('wetland',passf),
('layer', fix_layer),
]

place_props = common_props + [('population',passf), ('capital',yesNo)]

point_props = other_props + [('parent_highway',passf)]
poly_props = other_props

admin_props = common_props + [('admin_level',FixKey('min_admin_level'))]
*/

func init_feat_tags() {
    highway_prios := map[string]int64 {
        "railway_rail": 440,
        "railway_INT-preserved-ssy": 430,
        "railway_INT-spur-siding-yard": 430,
        "railway_subway": 420,
        "railway_narrow_gauge": 420,
        "railway_light_rail": 420,
        "railway_preserved": 420,
        "railway_funicular": 420,
        "railway_monorail": 420,
        "railway_miniature": 420,
        "railway_turntable": 420,
        "railway_tram": 410,
        "railway_tram-service": 405,
        "railway_disused": 400,
        "railway_construction": 400,
        "highway_motorway": 380,
        "highway_trunk": 370,
        "highway_primary": 360,
        "highway_secondary": 350,
        "highway_tertiary": 340,
        "highway_residential": 330,
        "highway_unclassified": 330,
        "highway_road": 330,
        "highway_living_street": 320,
        "highway_pedestrian": 310,
        "highway_raceway": 300,
        "highway_motorway_link": 240,
        "highway_trunk_link": 230,
        "highway_primary_link": 220,
        "highway_secondary_link": 210,
        "highway_tertiary_link": 200,
        "highway_service": 150,
        "highway_track": 110,
        "highway_path": 100,
        "highway_footway": 100,
        "highway_bridleway": 100,
        "highway_cycleway": 100,
        "highway_steps": 100,
        "highway_platform": 90,
        "railway_platform": 90,
        "aeroway_runway": 60,
        "aeroway_taxiway": 50,
        "highway_construction": 10,
        "highway_bus_guideway":10,
        "aerialway_cable_car":10,
        "aerialway_goods":10,
        "aerialway_chair_lift":10,
    }
    shopvals := map[string]string{"supermarket": "supermarket", "bakery": "bakery", "beauty": "beauty", "books": "books", "butcher": "butcher", "clothes": "clothes", "computer": "computer", "confectionery": "confectionery", "fashion": "fashion", "convenience": "convenience", "department_store": "department_store", "doityourself": "doityourself", "hardware": "hardware", "florist": "florist", "garden_centre": "garden_centre", "hairdresser": "hairdresser", "hifi": "hifi", "ice_cream": "ice_cream", "car": "car", "car_repair": "car_repair", "bicycle": "bicycle", "mall": "mall", "pet": "pet", "photo": "photo", "photo_studio": "photo_studio", "photography": "photography", "seafood": "seafood", "shoes": "shoes", "alcohol": "alcohol", "gift": "gift", "furniture": "furniture", "kiosk": "kiosk", "mobile_phone": "mobile_phone", "motorcycle": "motorcycle", "musical_instrument": "musical_instrument", "newsagent": "newsagent", "optician": "optician", "jewelry": "jewelry", "jewellery": "jewellery", "electronics": "electronics", "chemist": "chemist", "toys": "toys", "travel_agency": "travel_agency", "car_parts": "car_parts", "greengrocer": "greengrocer", "farm": "farm", "stationery": "stationery", "laundry": "laundry", "dry_cleaning": "dry_cleaning", "beverages": "beverages", "perfumery": "perfumery", "cosmetics": "cosmetics", "variety_store": "variety_store", "wine": "wine", "accessories": "minor", "antique": "minor", "antiques": "minor", "appliance": "minor", "art": "minor", "baby_goods": "minor", "bag": "minor", "bags": "minor", "bathroom_furnishing": "minor", "bed": "minor", "betting": "minor", "boat": "minor", "bookmaker": "minor", "boutique": "minor", "builder": "minor", "building_materials": "minor", "camera": "minor", "car_service": "minor", "carpet": "minor", "charity": "minor", "cheese": "minor", "chocolate": "minor", "coffee": "minor", "communication": "minor", "copyshop": "minor", "craft": "minor", "curtain": "minor", "dairy": "minor", "deli": "minor", "delicatessen": "minor", "discount": "minor", "dive": "minor", "e-cigarette": "minor", "electrical": "minor", "energy": "minor", "erotic": "minor", "estate_agent": "minor", "fabric": "minor", "fish": "minor", "fishing": "minor", "flooring": "minor", "food": "minor", "frame": "minor", "frozen_food": "minor", "funeral_directors": "minor", "furnace": "minor", "gallery": "minor", "gambling": "minor", "games": "minor", "gas": "minor", "general": "minor", "glaziery": "minor", "grocery": "minor", "health": "minor", "health_food": "minor", "hearing_aids": "minor", "herbalist": "minor", "hobby": "minor", "household": "minor", "houseware": "minor", "hunting": "minor", "insurance": "minor", "interior_decoration": "minor", "kitchen": "minor", "leather": "minor", "lighting": "minor", "locksmith": "minor", "lottery": "minor", "market": "minor", "massage": "minor", "medical": "minor", "medical_supply": "minor", "money_lender": "minor", "motorcycle_repair": "minor", "music": "minor", "office_supplies": "minor", "organic": "minor", "outdoor": "minor", "paint": "minor", "pastry": "minor", "pawnbroker": "minor", "pharmacy": "minor", "phone": "minor", "pottery": "minor", "printing": "minor", "radiotechnics": "minor", "real_estate": "minor", "religion": "minor", "rental": "minor", "salon": "minor", "scuba_diving": "minor", "second_hand": "minor", "sewing": "minor", "shoe_repair": "minor", "shopping_centre": "minor", "solarium": "minor", "souvenir": "minor", "sports": "minor", "tailor": "minor", "tanning": "minor", "tattoo": "minor", "tea": "minor", "ticket": "minor", "tiles": "minor", "tobacco": "minor", "trade": "minor", "tyres": "minor", "vacuum_cleaner": "minor", "video": "minor", "video_games": "minor", "watches": "minor", "wholesale": "minor", "yes": "minor", "": "other"}
    
    feat_tags = []featureSpec{
        &checkPrio{&passVal{&cutsuffix{&passcol{"highway"},"_link"},"point","otherline","polygon"},highway_prios,"highways"},
        &checkPrio{&passVal{&passcol{"railway"},"point","otherline","polygon"},highway_prios,"highways"},
        &checkPrio{&passVal{&passcol{"aeroway"},"point","otherline","polygon"},highway_prios,"highways"},
        &checkPrio{&passVal{&passcol{"aerialway"},"point","otherline","polygon"},highway_prios,"highways"},
        &passVal{&passcol{"landuse"},"point","otherline","polygon"},
        &passVal{&replval{&passcol{"shop"},shopvals},"point","","polygon"},
        &passVal{&passcol{"amenity"},"point","otherline","polygon"},
        &passVal{&passcol{"natural"},"point","otherline","polygon"},
        &passVal{&passcol{"barrier"},"point","otherline","polygon"},
        &passVal{&passcol{"power"},"point","otherline","polygon"},
        &passVal{&passcol{"route"},"point","otherline","polygon"},
        &passVal{&passcol{"man_made"},"point","otherline","polygon"},
        &passVal{&passcol{"leisure"},"point","otherline","polygon"},
        &passVal{&passcol{"historic"},"point","otherline","polygon"},
        &passVal{&passcol{"place"},"place","","polygon"},
        &passVal{&passcol{"historic"},"point","otherline","polygon"},
        &passVal{&passcol{"tourism"},"point","otherline","polygon"},
        &hasKey{"admin_boundary", &passVal{&passcol{"min_admin_level"},"","admin",""}},
        &passVal{&passcol{"military"},"point","otherline","polygon"},
        &passVal{&passcol{"boundary"},"point","","polygon"},
        &passVal{&passcol{"waterway"},"point","otherline","polygon"},
        &isBuilding{"polygon"},
        &hasKey{"", &passVal{&passcol{"addr:housename"},"point","",""}},
        &hasKey{"", &passVal{&passcol{"addr:housenumber"},"point","",""}},
    }
    
    commonProps := []colSpec{
        &joincol{&passcol{"name"},&passcol{"ele"}},
        &passcol{"ref"},
    }
    
    
    table_specs = map[string]tableSpec{}
    table_specs["highways"] = &tableSpecImpl{"highways", append(commonProps,[]colSpec{
            &replval{&passcol{"oneway"},map[string]string{"yes":"yes","-1":"-1"}},
            &renamecol{&hassuffix{&passcol{"highway"},"_link"},"link"},
            &yesno{&passcol{"bridge"}},
            &yesno{&passcol{"tunnel"}},
            &replval{&passcol{"access"},map[string]string{"destination":"","no":"","private":"no"}},
            &renamecol{&replval{&passcol{"surface"},map[string]string{"concrete:plates": "paved", "asphalt": "paved", "pebblestone": "unpaved", "earth": "unpaved", "compacted": "unpaved", "ground": "unpaved", "unpaved": "unpaved", "mud": "unpaved", "wood": "paved", "gravel": "unpaved", "cobblestone:flattened": "paved", "paved": "paved", "dirt": "unpaved", "grass_paver": "unpaved", "cobblestone": "paved", "woodchips": "unpaved", "paving_stones": "paved", "concrete:lanes": "paved", "metal": "paved", "sett": "paved", "concrete": "paved", "sand": "unpaved", "fine_gravel": "unpaved", "clay": "unpaved", "grass": "unpaved", "salt": "unpaved"}},"int_surface"},
            &replval{&passcol{"service"},map[string]string{"parking_aisle":"INT-minor","drive-through":"INT-minor","driveway":"INT-minor","":"INT-normal"}},
            &passcol{"bicycle"},
            &passcol{"construction"},
            &renamecol{&replval{&passcol{"construction"},map[string]string{"construction":"yes","footway":"yes","cycleway":"yes","bridleway":"yes","path":"yes","track":"yes","":"no"}},"int_construction_minor"},
            &passcol{"horse"},
            &passcol{"tracktype"},
            &renamecol{&passcol{"horse"},"layernotnull"},
            &passcol{"cycle_routes"},
            &passcol{"bus_routes"},
            &yesno{&passcol{"covered"}},
    }...), []colSpecMany{
        &splitcol{&renamecol{&passcol{"ref"},"refs"},";","height","width"},
    }}
   
    table_specs["otherline"] = &tableSpecImpl{"otherline", append(commonProps, []colSpec {
            &renamecol{&passcol{"addr:interpolation"},"addr_interpolation"},
            &yesno{&passcol{"tunnel"}},
            &yesno{&passcol{"bridge"}},
            &yesno{&passcol{"intermittent"}},
    }...),nil }
    table_specs["admin"] = &tableSpecImpl{"admin", append(commonProps, []colSpec {
            &renamecol{&passcol{"min_admin_level"},"admin_level"},
    }...),nil }
    
    
    otherProps := append(commonProps, []colSpec{
        &replval{&passcol{"religion"},map[string]string{"christian":"christian","jewish":"jewish","":"INT-generic"}},
        &passcol{"denomination"},
        &passcol{"operator"},
        &passcol{"access"},
        &renamecol{&passcol{"addr:housenumber"},"addr_housenumber"},
        &renamecol{&passcol{"addr:housename"},"addr_housename"},
        &haseither{"is_wind", []colSpec{&isval{&passcol{"generator:source"},"wind"},&isval{&passcol{"power_source"},"wind"}}},
        &passcol{"wetland"},
        &asint{&passcol{"layer"}},
    }...)
    
    table_specs["place"] = &tableSpecImpl{"place", append(commonProps, []colSpec {
            &passcol{"population"},
            &yesno{&passcol{"capitial"}},
    }...),nil }
    
    
    table_specs["polygon"] = &tableSpecImpl{"polygon", append(otherProps, []colSpec {
            &wayArea{},
    }...),nil }
    
    
    
    table_specs["point"] = &tableSpecImpl{"point", append(otherProps, []colSpec {
            &passcol{"parent_highway"},
    }...),nil }
    
    
    
}
    

func processObj(ele elements.Element) ([]elements.Element, error) {
    if len(feat_tags)==0 {
        init_feat_tags()
    }
    
    geom, err := geometry.ExtractGeometry(ele)
    if err!=nil { return nil, err}
    
    
    feats := featureSlice{}
    for _,v:=range feat_tags {
        f := v.Handle(geom)
        if f!=nil {
            feats=append(feats, f)
        }
    }
    
    if len(feats) == 0 {
        return nil,nil
    }
    
    ans := make([]elements.Element, 0, len(feats))
    
    if len(feats)>1 {
        tt := map[string]featureSlice{}
        for _,f:=range feats {
            tt[f.Table()] = append(tt[f.Table()], f)
        }
        
        nfeats := make(featureSlice,0,len(feats))
        for k,v:=range tt {
            if len(v)==1 {
                nfeats=append(nfeats, v[0])
            } else {
                v.Sort()
                nfeats=append(nfeats, &groupFeature{k, v})
            }
        }
        feats = nfeats
        
    }
    feats.Sort()
    
    gd := geom.GeometryData()
    for _,f := range feats {
        if (f.Feature(0)=="") && (len(feats)>1) {
            //skip
        } else {
            ts := table_specs[f.Table()]
            nt := ts.Tags(f, geom)
            ans = append(ans, elements.MakeGeometry(geom.Id(), geom.Info(), nt, gd, geom.Quadtree(), geom.ChangeType()))
        }
        
    }
    return ans, nil
}

func makeGeoms(inc <-chan elements.ExtendedBlock) <-chan elements.ExtendedBlock {
    outc := make(chan elements.ExtendedBlock)
    go func() {
        ii:=0
        for bl := range inc {
            feats := make(elements.ByElementId, 0, bl.Len())
            for i:=0; i < bl.Len(); i++ {
                ele  := bl.Element(i)
                ff, err := processObj(ele)
                if err!=nil {
                    fmt.Println("??", bl.Quadtree(), i, ele, err.Error())
                } else if ff!=nil {
                    feats=append(feats, ff...)
                }
            }
            if len(feats)>0 {
                outc <- elements.MakeExtendedBlock(ii, feats, bl.Quadtree(), bl.StartDate(), bl.EndDate(), bl.Tags())
                ii+=1
            }
        }
        close(outc)
    }()
    return outc
}
                
            
            
        

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU()*2)
    cleanQueryString = regexp.MustCompile("[\n\t ]+")
        
    commonstrs := []string{}
    
    prfx := flag.String("p","","prfx")
    endstr := flag.String("e","","enddate")
    filt   := flag.String("f","planet","filter")
    stylefn := flag.String("style","extrastyle.json","stylefn")
    //queriesfn:= flag.String("queries",/*"/home/james/map_data/openstreetmap/openstreetmap-carto/project-pyds.mml"*/"","queriesfn")
    commonstrings:= flag.String("commonstrings","","common strings")
    //stats := flag.Bool("stats", false, "show stats")
    outFile := flag.String("o", "", "output file")
    recalc := flag.Bool("recalc",false,"recalc qts")
    sortBlocks := flag.Bool("sort",false,"sort")
    //viewsFn := flag.String("views","","viewsfn");
    prog:=flag.Bool("prog",false,"prog")
    
    flag.Parse()
    
    if (*prfx)=="" {
        panic("must specify file prefix")
    }
    
    if *outFile == "" {
        panic("No outfile specified")
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
    
    fmt.Println("outfn=",*outFile)    
      
    
    if *commonstrings != "" {    
        sf,err:=os.Open("planet-strings.json")
        if err!=nil { panic(err.Error()) }
        err = json.NewDecoder(sf).Decode(&commonstrs)
        if err!=nil { panic(err.Error()) }
        if len(commonstrs) > 30000 {
            panic("too many strings!")
        }
        
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
  
    
    tagsFilter,err := geometry.ReadStyleFile(*stylefn)
    if err!=nil { panic(err.Error()) }
    
    fbx:=locTest.Bbox()
    geometries,err := geometry.GenerateGeometries(makeInChan, &fbx, tagsFilter, *recalc, true)
    if err!=nil { panic(err.Error())}
    
    geometries = makeGeoms(geometries)
    
    if (*prog) {
        geometries = geometryProgress(geometries, 1371)
    }
    
    
    
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
}
