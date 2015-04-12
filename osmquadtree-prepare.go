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



func progress(inc <-chan elements.ExtendedBlock, nb int, suf string) chan elements.ExtendedBlock {
    //incc := inc
    outc := make(chan elements.ExtendedBlock)
    
    go func() {
        //println("progress called",suf)
        cc:=0
        nc:=0
        st:=time.Now()
        mm:=""
        
        for bl := range inc {
            
            mm = fmt.Sprintf("\r%s%8.1fs %6d: %8d %s",suf,time.Since(st).Seconds(),bl.Idx(),cc,bl)
            cc += bl.Len()
            //println(mm)
            if cc > nc {
                fmt.Printf("%s %s",mm,utils.MemstatsStr())
                mm=""
                nc += nb
            }
            outc <- bl
        }
        fmt.Println(mm)
        close(outc)
    }()
    return outc
}

func main() {
    runtime.GOMAXPROCS(runtime.NumCPU()*2)
        
    utils.OnTerm()
        
    
    infn  := flag.String("i","planet-latest.osm.pbf","input pbf file")
    prfx  := flag.String("p","planet/", "prefix")
    eds   := flag.String("e", "", "timestamp")
    
    onlyCalcQts     := flag.Bool("onlycalcqts", false, "only calc qts")
    onlyCalcGroups  := flag.Bool("onlycalcgroups", false, "only calc groups")
    writeQtTree     := flag.Bool("writeqttree", false, "write qt tree to file")
    inMem           := flag.Bool("inmem", false, "sort objs in memory")
    writeGroups     := flag.Bool("writegroups",false,"write quadtree groups to file")

    qtTreeMaxLevel    := flag.Int("qttreemaxlevel",17,"round qt values (default 17)")
    target := flag.Int("target",8000,"target group size")
    minimum := flag.Int("minimum",4000,"minimum group size")
    
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
    
    filestr := endDate.FileString(endDate % (24*60*60) == 0)
        
    qtfn  := *prfx + filestr+"-qts.pbf"
    outfn := *prfx + filestr+".pbf"

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
        
        
        qtt := calcqts.FindQtTree(df, uint(*qtTreeMaxLevel))
        fmt.Printf("created qttree: %d items [%s]\n", qtt.Len(), utils.MemstatsStr())
        if *writeQtTree {
            qtfa,_ := os.Create(*prfx+filestr+"-qts.txt")
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
            qtf,_ = os.Create(*prfx+filestr+"-groups.txt")
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
    
    
    addQtBlocks, err := readfile.AddQts(*infn,qtfn,4)
    if err!=nil {
        panic(err.Error())
    }
    
    spt := "tempfilesplit"
    if *inMem {
        spt = "inmem"
    }
    
    nqb:=make([]chan elements.ExtendedBlock, 4)
    
    for i,q:=range addQtBlocks {
        if i==0 {
            nqb[i] = progress(q,137*8000/4,"read ")
        } else {
            nqb[i] = q
        }
    }
    
    
    
    outChans, err := blocksort.SortElementsByAlloc(/*addQtBlocks*/nqb,alloc,4,makeBlock,spt)
    if err!=nil {
        panic(err.Error())
    }
        
    //fmt.Println("sort and write to ",outfn)
    //sorted := readfile.CollectExtendedBlockChans(outChans,false)
    
    st:=time.Now()
    _,err = writefile.WritePbfFileM(outChans,outfn,false)
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
