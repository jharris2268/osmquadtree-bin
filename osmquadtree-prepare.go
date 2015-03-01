package main

import (
    "github.com/jharris2268/osmquadtree/calcqts"
    "github.com/jharris2268/osmquadtree/writefile"
    "github.com/jharris2268/osmquadtree/readfile"
    "github.com/jharris2268/osmquadtree/utils"
    "github.com/jharris2268/osmquadtree/elements"
    "github.com/jharris2268/osmquadtree/quadtree"
    "github.com/jharris2268/osmquadtree/blocksort"
    
    "os"
    "runtime"
    "fmt"
    "runtime/debug"
    "time"
    "flag"
)

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU()*2)
    
    
    infn  := flag.String("i","planet-latest.osm.pbf","input pbf file")
    prfx  := flag.String("p","planet/", "prefix")
    eds   := flag.String("e", "", "timestamp")
    
    onlyCalcQts     := flag.Bool("onlycalcqts", false, "only calc qts")
    onlyCalcGroups  := flag.Bool("onlycalcgroups", false, "only calc groups")
    writeQtTree     := flag.Bool("writeqttree", false, "write qt tree to file")
    
    flag.Parse()
    
    endstr := *eds
    
    var err error
    endDate := elements.Timestamp(0)
    if endstr != "" {
        endDate,err = elements.ReadDateString(endstr)
        if err!=nil {
             panic(err.Error())
        }
    }
    
    _,head,err := readfile.GetHeaderBlock(*infn)
    if head != nil && endDate == 0 {
        fmt.Println("use header timestamp", head.Timestamp)
        endDate=head.Timestamp
        endstr = endDate.String()
    }
    
    if endDate == 0 {
        fmt.Println("??", endDate, endstr)
        panic(err.Error())
    }
    
    qtfn  := *prfx + endstr+"-qts.pbf"
    outfn := *prfx + endstr+".pbf"

    fmt.Println(*infn,*prfx,*eds,"=>",endDate,qtfn,outfn)

    var t0,t1,t2,t3 float64
    t0 = 0.0
    if ex,_:=utils.FileExists(qtfn); !ex || *onlyCalcQts {
        
        st:=time.Now()
        
        qq,err := calcqts.CalcObjectQts(*infn)
        if err!=nil {
            panic(err.Error())
        }
        
        err = writefile.WriteQts(qq,qtfn)

        if err!=nil {
            panic(err.Error())
        }
        
        t0 = time.Since(st).Seconds()
        
        if *onlyCalcQts {
            fmt.Printf("Calc qts:     %8.1fs\n",t0)
            return
        }
        
    }
    
    
    
    qts:=func() []quadtree.Quadtree {
        st:=time.Now()
        
        df,err := readfile.ReadQtsMulti(qtfn,4)
        if err!=nil { panic(err.Error()) }
        qtt := calcqts.FindQtTree(df, 17)
        
        if *writeQtTree {
            qtfa,_ := os.Create(*prfx+endstr+"-qts.txt")
            i:=0
            for g:=range qtt.Iter() {
                qtfa.WriteString(fmt.Sprintf("%6d %s\n",i,g))
                i++
            }
            qtfa.Close()
        }
        
        t1 = time.Since(st).Seconds()
        st=time.Now()
        
        grps := calcqts.FindQtGroups(qtt, 8000)
        
        qtf,_ := os.Create(*prfx+endstr+"-groups.txt")
        
        qts:=make([]quadtree.Quadtree, 0, 500000)
        i:=0
        for g:=range grps.Iter() {
            qts=append(qts, g.Quadtree)
            qtf.WriteString(fmt.Sprintf("%6d %s\n",i,g))
            i++
        }
        qtf.Close()
        
        fmt.Printf("have %d qts...\n",len(qts))
        debug.FreeOSMemory()
        t2 = time.Since(st).Seconds()
        return qts
    }()
    
    if *onlyCalcGroups {
        fmt.Printf("Calc qts:     %8.1fs\n",t0)
        fmt.Printf("Make qt tree: %8.1fs\n",t1)
        fmt.Printf("Find groups:  %8.1fs\n",t2)
        return 
    }
    
    qtt:=calcqts.MakeQtTree(qts)
    
    qtm:=map[quadtree.Quadtree]int{}
    for i,q:=range qts{
        qtm[q]=i
    }
    
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
    
    addQtBlocks, err := readfile.AddQts(*infn,qtfn,4)
    if err!=nil {
        panic(err.Error())
    }
    
    outChans, err := blocksort.SortElementsByAlloc(addQtBlocks,alloc,4,makeBlock,"tempfilesplit")
    if err!=nil {
        panic(err.Error())
    }
        
    fmt.Println("sort and write to ",outfn)
    sorted := readfile.CollectExtendedBlockChans(outChans,false)
    
    st:=time.Now()
    _,err = writefile.WritePbfFile(sorted,outfn,true,false,false)
    if err!=nil {
        panic(err.Error())
    }
    
    t3=time.Since(st).Seconds()
    //fmt.Println(ii)    
    fmt.Println("Done!")
    
    fmt.Printf("Calc qts:     %8.1fs\n",t0)
    fmt.Printf("Make qt tree: %8.1fs\n",t1)
    fmt.Printf("Find groups:  %8.1fs\n",t2)
    fmt.Printf("Write tiles:  %8.1fs\n",t3)
    fmt.Printf("Total:        %8.1fs\n",t0+t1+t2+t3)
}
