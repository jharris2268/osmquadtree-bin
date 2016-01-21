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
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/blocksort"
    "github.com/jharris2268/osmquadtree/calcqts"
    "github.com/jharris2268/osmquadtree/writefile"
    "github.com/jharris2268/osmquadtree/utils"
    "github.com/jharris2268/osmquadtree/quadtree"
    "github.com/jharris2268/osmquadtree/locationscache"
	
	
    "flag"
    "fmt"
	"time"
	"runtime"
    "runtime/debug"
    "os"
	"strings"
    //"sync"
)


func progress(inc chan elements.ExtendedBlock, ii int) chan elements.ExtendedBlock {
    
    res := make(chan elements.ExtendedBlock)
    go func() {
        st:=time.Now()
        ll := ""
        li := 0
        nn := 0
        var b elements.ExtendedBlock
        
        for b = range inc {
            
            li += b.Len()
            if li > nn {
                ll = fmt.Sprintf("%-8.1fs: %6d %0.100s %s", time.Since(st).Seconds(), b.Idx(), b, utils.MemstatsStr()) 
                fmt.Printf("\r%s",ll)
                nn+=ii
            }
            res <- b
            
        }
        
        ll = fmt.Sprintf("%-8.1fs: %6d %0.100s %s", time.Since(st).Seconds(), b.Idx(), b, utils.MemstatsStr()) 
        fmt.Printf("\r%s\n",ll)
        
        
        close(res)
    }()
    return res
}

func nodesAndWays(inchan <-chan elements.ExtendedBlock,dropemptyrels bool) chan elements.ExtendedBlock {
    finishedchan := make(chan pendingBlock)
    res:=make(chan elements.ExtendedBlock)
    go func() {
        pending := map[quadtree.Quadtree]pendingBlock{}
        ri:=-1
        for bl := range inchan {
            qt := bl.Quadtree()
            findFinished(pending, qt, finishedchan)
            
            rr:= makePendingBlock(pending, bl, dropemptyrels)
            if len(rr)>0 {
                res <- elements.MakeExtendedBlock(ri, rr, bl.Quadtree(),0,0,nil)
                ri-=1
            }
        }
        findFinished(pending, -1, finishedchan)
        
        close(finishedchan)
        
    }()
    
    
    
    go func() {
        mxidx:=-1
        
        for pb := range finishedchan {
            
            bl := make(elements.ByElementId, 0, len(pb.nodes)+len(pb.ways))
            
            
            
            for _,nd := range pb.nodes {
                nq,ok := pb.nodeqts[nd.Id()]
                if !ok || nq==quadtree.Null {
                    nq,_ = quadtree.Calculate(quadtree.Bbox{nd.Lon(), nd.Lat(), nd.Lon()+1, nd.Lat()+1}, 0.05, 18)
                    pb.nodeqts[nd.Id()] = nq
                }
                
                
                nd.SetQuadtree(nq)
                bl = append(bl, nd)
            }
            for _,w:=range pb.ways {
                wf:=elements.Ref(1<<59) | w.Id()
                
                pb.nodeqts[wf]=w.Quadtree()
                
                bl=append(bl,w)
            }
            
            res <- elements.MakeExtendedBlock(pb.idx,bl,quadtree.Null,0,0,nil)
            if pb.idx>mxidx {
                mxidx=pb.idx
            }
        }
        close(res)
    }()
    return res
}

  
type relp struct {
    a,b elements.Ref
}
func relations(inc chan elements.ExtendedBlock, nc int, msgs bool) []chan elements.ExtendedBlock {
    res := make([]chan elements.ExtendedBlock, nc)
    for i,_:=range res {
        res[i]=make(chan elements.ExtendedBlock)
    }
    
    go func() {
        nn := map[elements.Ref][]elements.Ref{}
        ww := map[elements.Ref][]elements.Ref{}
        
        rr := map[elements.Ref]quadtree.Quadtree{}
        rp := []relp{}
        pending := map[elements.Ref]elements.FullRelation{}
        cc := map[elements.Ref]int{}
        hasr := map[elements.Ref]bool{}
        
        zz:=0
        
        tl:=0
        nt:=784341
        st:=time.Now()
        
        for bl := range inc {
            if msgs && tl>nt {
                fmt.Printf("\r%-10.1fs %6d %15d %6d [%5d] %6d pending {%8d n, %8d w, %8d r, %8d rm} %s",time.Since(st).Seconds(),zz, tl,bl.Idx(),bl.Len(), len(pending), len(nn),len(ww),len(rr), len(rp),utils.MemstatsStr())  
                nt+=784341
            }
            
            ff := make(elements.ByElementId, 0, bl.Len()*3/2)
            if bl.Idx()<0 {
                for i:=0; i < bl.Len(); i++ {
                    rel,ok:=bl.Element(i).(elements.FullRelation)
                    if !ok {
                        println("???", bl.Element(i).String())
                    }
                    if rel.Len()==0 {
                        rel.SetQuadtree(0)
                        ff=append(ff, rel)
                        rr[rel.Id()] = 0
                        
                    } else {
                    
                        c:=0
                        hr:=false
                        
                        for j:=0; j < rel.Len(); j++ {
                            switch rel.MemberType(j) {
                                case elements.Node:
                                    nn[rel.Ref(j)] = append(nn[rel.Ref(j)], rel.Id())
                                    c++
                                case elements.Way:
                                    ww[rel.Ref(j)] = append(ww[rel.Ref(j)], rel.Id())
                                    c++
                                case elements.Relation:
                                    hr=true
                                    if rel.Ref(j)==rel.Id() {
                                        fmt.Println("Circular rel:",rel, "at", j, rel.Ref(j))
                                    } else {
                                        rp = append(rp, relp{rel.Id(), rel.Ref(j)})
                                    }
                            }
                        }
                        
                        pending[rel.Id()] = rel
                        cc[rel.Id()] = c
                        if hr {
                            hasr[rel.Id()] = true
                        }
                        rr[rel.Id()] = quadtree.Null
                    }
                }
            } else {
                
                for i:=0; i < bl.Len(); i++ {
                    e:=bl.Element(i)
                    
                    qtt,ok:=e.(elements.Quadtreer)
                    if !ok {
                        println("???", e.String())
                    }
                    q:=qtt.Quadtree()
                    ff=append(ff,e)
                    switch e.Type() {
                        case elements.Node:
                            if ll,ok := nn[e.Id()]; ok {
                                for _,r:=range ll {
                                    rr[r] = rr[r].Common(q)
                                    cc[r] -= 1
                                    
                                    if cc[r]==0 {
                                        if _,ok := hasr[r]; !ok {
                                            rl:=pending[r]
                                            rl.SetQuadtree(rr[r])
                                            ff=append(ff,rl)
                                            delete(pending, r)
                                            
                                        }
                                        delete(cc, r)
                                    }   
                                }
                                delete(nn,e.Id())
                            }
                        case elements.Way:
                            if ll,ok := ww[e.Id()]; ok {
                                for _,r:=range ll {
                                    rr[r] = rr[r].Common(q)
                                    cc[r] -= 1
                                    
                                    if cc[r]==0 {
                                        if _,ok := hasr[r]; !ok {
                                            rl:=pending[r]
                                            rl.SetQuadtree(rr[r])
                                            ff=append(ff,rl)
                                            delete(pending, r)
                                            
                                        }
                                        delete(cc, r)
                                    }
                                }
                                delete(ww,e.Id())
                            }
                        default:
                            println("???", e.String())
                    }
                    
                }
            }
            if len(ff)>0 {
                tl+=len(ff)
                res[zz%nc] <- elements.MakeExtendedBlock(zz, ff, quadtree.Null,0,0,nil)
                zz++
            }
        }
        if msgs { fmt.Printf("\r") }
        fmt.Printf("%-10.1fs %6d %15d %6d [%5d] %6d pending {%8d n, %8d w, %8d r, %8d rm} %s\n",time.Since(st).Seconds(),zz, tl,-1,0, len(pending), len(nn),len(ww),len(rr), len(rp),utils.MemstatsStr()) 
        
        for i:=0; i < 5; i++ {
            for _,r:=range rp {
                sq,ok := rr[r.b]
                if ok && sq!=quadtree.Null {
                    rr[r.a] = rr[r.a].Common(sq)
                }
            }
        }
/*            for k,vv:=range rr {
                switch k {
                    case 20990, 21359, 63910, 72927, 177976, 371559, 2133091, 2147162, 2749877, 2763798, 4625716:
                        println(i, k, rq[k].String(),len(vv))
                }
                
                for _,v:=range vv {
                    sq,ok := rq[v]
                    if ok && sq !=quadtree.Null {
                        rq[k] = rq[k].Common(sq)
                        switch k {
                            case 20990, 21359, 63910, 72927, 177976, 371559, 2133091, 2147162, 2749877, 2763798, 4625716:
                                println(v,sq.String(), rq[k].String())
                            
                        }
                    }
                }
            }*/
        
        
        
        fin:=make(elements.ByElementId, 0, len(pending))
        for _,rel := range pending {
            q := rr[rel.Id()]
            if q==quadtree.Null {
                q=0
            }
            rel.SetQuadtree(q)
            fin=append(fin, rel)
        }
        if len(fin)>0 {
            res[zz%nc] <- elements.MakeExtendedBlock(zz,fin,quadtree.Null,0,0,nil)
        }
        for _,r:=range res {
            close(r)
        }
    }()
    return res
}
    
    
    
        
        
type pendingBlock struct {
    idx int
    nodes map[elements.Ref]elements.FullNode
    ways  map[elements.Ref]elements.FullWay
    
    nodeqts map[elements.Ref]quadtree.Quadtree
}

func findFinished(pending map[quadtree.Quadtree]pendingBlock, q quadtree.Quadtree, finishedchan chan pendingBlock) {
    pp:=make([]quadtree.Quadtree,0,len(pending))
    for k,v:=range pending {
        if q<0 || k.Common(q)!=k {
            pp=append(pp,k)
            finishedchan <- v
        }
        
    }
    for _,p:=range pp {
        delete(pending, p)
    }
}

func findNodeLoc(pending map[quadtree.Quadtree]pendingBlock, pb pendingBlock, rf elements.Ref) (int64,int64) {
    n,ok := pb.nodes[rf]
    if ok {
        return n.Lon(),n.Lat()
    }
    
    for _,v:=range pending {
        n,ok = v.nodes[rf]
        if ok {
            return n.Lon(),n.Lat()
        }
    }
    fmt.Println("can't find node ??",rf)
    return 0,0
}

func setNodeQt(pending map[quadtree.Quadtree]pendingBlock, pb pendingBlock, rf elements.Ref, q quadtree.Quadtree) {
    n,ok := pb.nodeqts[rf]
    if ok {
        pb.nodeqts[rf] = n.Common(q)
        return 
    }
    for k,v:=range pending {
        n,ok = v.nodeqts[rf]
        if ok {
            pending[k].nodeqts[rf] = n.Common(q)
            return
        }
    }
    fmt.Println("can't find node ??",rf)
}


func makePendingBlock(pending map[quadtree.Quadtree]pendingBlock, bl elements.ExtendedBlock, dropemptyrels bool) elements.ByElementId {
    pb:=pendingBlock{bl.Idx(),
        map[elements.Ref]elements.FullNode{},
        map[elements.Ref]elements.FullWay{},
        map[elements.Ref]quadtree.Quadtree{}}
        
    rr := make(elements.ByElementId,0,bl.Len())
    
    for i:=0; i < bl.Len(); i++ {
        e:=bl.Element(i)
        switch e.Type() {
            case elements.Node:
                pb.nodes[e.Id()] = e.(elements.FullNode)
                pb.nodeqts[e.Id()] = quadtree.Null
            case elements.Way:
                bb:=quadtree.NullBbox()
                w:=e.(elements.FullWay)
                for j:=0; j < w.Len(); j++ {
                    ln,lt := findNodeLoc(pending,pb,w.Ref(j))
                    bb.ExpandXY(ln,lt)
                }
                qt,_:=quadtree.Calculate(*bb, 0.05, 18)
                w.SetQuadtree(qt)
                for j:=0; j < w.Len(); j++ {
                    setNodeQt(pending,pb,w.Ref(j),qt)
                }
                pb.ways[w.Id()] = w
            case elements.Relation:
                r:=e.(elements.FullRelation)
                if !dropemptyrels || r.Len()>0 {
                    rr=append(rr,r)
                }
        }
    }
    
    pending[bl.Quadtree()] = pb
    return rr
}

type objsIdx struct {
    vv []byte
    ii []int
}

func addObj(oi map[int]*objsIdx, e elements.Element) {
    
    last:=oi[len(oi)-1]
    
    p := e.Pack()
    
    if len(p)+len(last.vv) > cap(last.vv) {
        oi[len(oi)] = &objsIdx{make([]byte,0,1<<20),make([]int, 0,2000)}
        last = oi[len(oi)-1]
    }            
    
    last.ii = append(last.ii, len(last.vv))
    last.vv = append(last.vv, p...)
}

func iterObjs(oi map[int]*objsIdx) chan elements.FullRelation {
    res:=make(chan elements.FullRelation)
    go func() {
        for i:=0; i < len(oi); i++ {
            for j:=0; j < len(oi[i].ii); j++ {
                l:=oi[i].ii[j]
                m:=len(oi[i].vv)
                if j+1 < len(oi[i].ii) {
                    m=oi[i].ii[j+1]
                }
                
                e := elements.UnpackElement(oi[i].vv[l:m])
                res <- e.(elements.FullRelation)
            }
            
        }
        close(res)
    }()
    return res
}
 

func recalcQts(inchan <-chan elements.ExtendedBlock, nc int, dropemptyrels bool, mm bool) []chan elements.ExtendedBlock {
    return relations(nodesAndWays(inchan, dropemptyrels), nc, mm)
}

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU()*2)
    
    //inpr := "/home/james/map_data/repl_diffs/british-isles/" //643.osc.gz"
    prfx    :=flag.String("p","planet/","data prefix")
    endstr  :=flag.String("e","","end date")
    
    outprfx :=flag.String("o","","new prefix")
    
    reCalcQts     := flag.Bool("recalcqts", false, "recalc qts")
    onlyCalcGroups  := flag.Bool("onlycalcgroups", false, "only calc groups")
    writeQtTree     := flag.Bool("writeqttree", false, "write qt tree to file")
    inMem           := flag.Bool("inmem", false, "sort objs in memory")
    writeGroups     := flag.Bool("writegroups",false,"write quadtree groups to file")

    qtTreeMaxLevel    := flag.Int("qttreemaxlevel",17,"round qt values (default 17)")
    target := flag.Int("target",8000,"target group size")
    minimum := flag.Int("minimum",4000,"minimum group size")
    dropemptyrels := flag.Bool("dropemptyrels", false, "drop empty rels")
    
    quadtreeTuple := flag.Bool("quadtreetupl",false,"write quadtree as tuple")
        
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
    
    laststate:=int64(0)
    
    if strings.HasSuffix(*prfx, ".pbf") {
        _,_,err := readfile.GetHeaderBlock(*prfx)
        if err!=nil {
            panic(err.Error())
        }
        
        
    } else {
        settings,err := locationscache.GetUpdateSettings(*prfx)
        if err!=nil {
            panic(err.Error())
        }        
        
        var ii []locationscache.IdxItem
        ii,_,err = locationscache.GetCacheSpecs(*prfx,settings.LocationsCache)        
        if err!=nil {
            panic(err.Error())
        }
        
        origfn = *prfx+ii[0].Filename
        
        chgfns = make([]string,0,len(ii)-1)
        if len(ii)>1 {
            for j,i := range ii {
                if j==0 {
                    continue
                }
                
                if endDate>0 && (i.Timestamp > endDate) {
                    laststate = ii[j-1].State
                    endDate  = ii[j-1].Timestamp
                    break
                }
                chgfns = append(chgfns,*prfx+i.Filename)
            }
            if len(chgfns) > 0 {
                fmt.Printf("origfn:%s, %d changes [%s=>%s]\n",origfn, len(chgfns), chgfns[0], chgfns[len(chgfns)-1])
            }
        }
        if laststate==0 {
            laststate = ii[len(ii)-1].State
        }
        if endDate==0 {
            endDate  = ii[len(ii)-1].Timestamp
        }
    }
    if endDate == 0 {
        panic("must specify enddate")
    }
    
    outp := *prfx
    if *outprfx != "" {
        outp = *outprfx
    }
    
    filestr := fmt.Sprintf("%s%s", outp, endDate.FileString((endDate%(24*60*60))==0))
    outfn := filestr + ".pbf"
    
    
    fmt.Printf("endDate=%d, hh/mm/ss=%d,laststate=%d,filestr=%s\n",int(endDate),int(endDate%(24*60*60)),laststate,filestr)
    
    
    makeInChan := func(isf bool) []chan elements.ExtendedBlock {
        rr,err := readfile.ReadExtendedBlockMultiMerge(origfn, chgfns, 4)
        if err!=nil {
            panic(err.Error())
        }
        if *reCalcQts {
            return recalcQts(readfile.CollectExtendedBlockChans(rr),len(rr),*dropemptyrels, true)
        }
        return rr
    }
    
    t1,t2,t3 := 0.0,0.0,0.0
    
    qts:=func() []quadtree.Quadtree {
        st:=time.Now()
        
        //df,err := readfile.ReadQtsMulti(qtfn,4)
        //if err!=nil { panic(err.Error()) }
        dfn := makeInChan(true)
        df := make([]chan elements.ExtendedBlock, len(dfn))
        for i,d:=range dfn {
            if i==0 {
                df[i]=progress(d,137*8000/4)
            } else {
                df[i]=d
            }
        }
        
        
        qtt := calcqts.FindQtTree(df, uint(*qtTreeMaxLevel))
        fmt.Printf("created qttree: %d items [%s]\n", qtt.Len(), utils.MemstatsStr())
        if *writeQtTree {
            qtfa,_ := os.Create(filestr+"-qts.txt")
            i:=0
            for g:=range qtt.Iter() {
                qtfa.WriteString(fmt.Sprintf("%6d %s\n",i,g))
                i++
            }
            qtfa.Close()
        }
        
        t1 = time.Since(st).Seconds()
        st=time.Now()
        
        grps := calcqts.FindQtGroups(qtt, int64(*target), int64(*minimum))
        var qtf interface {
            WriteString(string) (int, error)
            Close() error
        }
        if *writeGroups {
            qtf,_ = os.Create(filestr+"-groups.txt")
            defer qtf.Close()
        }
        qts:=make([]quadtree.Quadtree, 0, 500000)
        i:=0
        for g:=range grps.Iter() {
            qts=append(qts, g.Quadtree)
            if qtf!=nil {
                qtf.WriteString(fmt.Sprintf("%6d %s\n",i,g))
            }
            i++
        }
        
        
        
        fmt.Printf("have %d qts [%s]\n",len(qts),utils.MemstatsStr())
        debug.FreeOSMemory()
        t2 = time.Since(st).Seconds()
        return qts
    }()
    
    if *onlyCalcGroups {
        //fmt.Printf("Calc qts:     %8.1fs\n",t0)
        fmt.Printf("Make qt tree: %8.1fs\n",t1)
        fmt.Printf("Find groups:  %8.1fs\n",t2)
        return 
    }
    
    qtt:=calcqts.MakeQtTree(qts)
    
    qtm:=map[quadtree.Quadtree]int{}
    for i,q:=range qts{
        qtm[q]=i
    }
    
    fmt.Printf("created alloc quadtree: %d items [%s]\n",qtt.Len(),utils.MemstatsStr())
    
    alloc := func(e elements.Element) int {
        qt:=e.(elements.Quadtreer).Quadtree()
        gi:=qtt.Find(qt)
        g:=qtt.At(gi).Quadtree
        a,ok:=qtm[g]
        if !ok {
            return len(qtm)
        }
        return a
    }
    
    
    
    makeBlock := func(i int, a int, bl elements.Block) (elements.ExtendedBlock,error) {
        qt := quadtree.Null
        if a<0 || a>= len(qts) {
            fmt.Println("????", a, len(qts))
        } else {
            qt = qts[a]
        }
            
        return elements.MakeExtendedBlock(i,bl,qt,0,endDate,nil),nil
        
    }
    
    
    /*
    addQt := func(addf func(int, elements.ExtendedBlock) error) error {
        st:=time.Now()
        addff := func(i int, bl elements.ExtendedBlock) error {
            if (bl.Idx() % 1271)==0 {
                fmt.Printf("%-8.1f %-6d %s %s\n", time.Since(st).Seconds(), bl.Idx(), bl,utils.MemstatsStr())
            }
            return addf(i,bl)
        }
        
        return readfile.AddQts(*infn,qtfn,4,addff)
    }*/
    
    addQtBlocks := makeInChan(false)
    
    spt := "tempfilesplit"
    if *inMem {
        spt = "inmem"
    }
    
    
    
    if !(*reCalcQts) {
        nqb:=make([]chan elements.ExtendedBlock, 4)
        for i,q:=range addQtBlocks {
            if i==0 {
                nqb[i] = progress(q,137*8000/4)
            } else {
                nqb[i] = q
            }
        }
        addQtBlocks=nqb
    }   
    
    outChans, err := blocksort.SortElementsByAlloc(addQtBlocks,alloc,4,makeBlock,spt)
    if err!=nil {
        panic(err.Error())
    }
    
    st:=time.Now()
    _,err = writefile.WritePbfFile(outChans,outfn,false,*quadtreeTuple)
    if err!=nil {
        panic(err.Error())
    }
    
    t3=time.Since(st).Seconds()
    //fmt.Println(ii)    
    fmt.Println("Done!")
    
    fmt.Println("laststate=",laststate,"endDate=",endDate.String())
    
    //fmt.Printf("Calc qts:     %8.1fs\n",t0)
    fmt.Printf("Make qt tree: %8.1fs\n",t1)
    fmt.Printf("Find groups:  %8.1fs\n",t2)
    fmt.Printf("Write tiles:  %8.1fs\n",t3)
    fmt.Printf("Total:        %8.1fs\n",t1+t2+t3)
    
    
        
}

