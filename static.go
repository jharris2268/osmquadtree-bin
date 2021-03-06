
package main

import (
    "net/http"
)

func returnIndex(rw http.ResponseWriter, req *http.Request) {
    rw.Header().Set("Content-Type", "text/html")
    rw.Write([]byte(`<!DOCTYPE html>
<html>
<head>
	<title>osmquadtree-filter</title>
	<meta charset="utf-8" />

	<meta name="viewport" content="width=device-width, initial-scale=1.0">

    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet.draw/0.2.3/leaflet.draw.css" />
	<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.3/leaflet.css" />
        
    <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/2.1.3/jquery.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery.fileDownload/1.4.2/jquery.fileDownload.min.js"></script>
    
    
	<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.3/leaflet.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet.draw/0.2.3/leaflet.draw.js"></script>
    
    <style>
        ul { list-style-type: none; margin: 0px; padding: 0px; }
        h4 { margin: 0px; padding: 0px; }
        
        #body { top: 55px; bottom: 55px; width: 100%; position: absolute;}
        .center { height: 100%; right: 0px; left: 0px; }
        .right { float: right; top: 0px; }
        .left { float: left; }
        .footer { float: bottom; width: 100%; bottom: 0px; position: absolute;}
        //.row { width: 100%; }
        .bbox_input { width: 40px; }
        .scroll-y { overflow-y: scroll; }
        
        .icon { width: 20px; height: 20px; background-color: red; float: right; }
        //#polycoords { height: 200px; overflow-y: auto;}
        
        #boundspanel { width: 200px; }
        #quadtreepanel {width: 400px; }
        .tileinfo { text-size: 10; }
        .tileinfo .object { background-color: green; }
        .tileinfo .parent { background-color: blue; }
        
    </style>
    
</head>
<body>
    
    <div id="header" class="header row float">
        <h1>osmquadtree-filter</h1>
	</div>
    <div id="footer" class="footer row">...</div>
    <div id="body" class ="body row">
        <div id="panel" class="left body col">
            
            <ul>
                <li class="button"><button onclick="makeBbox()">Make Bbox</button></li>
                <li class="button"><button onclick="makePolygon()">Make Polygon</button></li>
                <li class="button"><button onclick="clearBounds()">Clear Bounds</button></li>
                <li id="bbox" class="button" style="height: 100px;">
                    <table style="width: 100%; height: 100%">
                    <tr>
                        <td/>
                        <td><input id="top" class="bbox_input" type="text"/></td>
                        <td/>
                    </tr>
                    <tr>
                        <td><input id="left" class="bbox_input" type="text" name="left"/></td>
                        <td/>
                        <td><input id="right" class="bbox_input" type="text" name="right"/></td>
                    </tr>
                    <tr>
                        <td/>
                        <td><input id="bottom" class="bbox_input" type="text" name="bottom"/></td>
                        <td/>
                    </tr>
                    </table>
                </li>
                <li class="button" id="polycoords"></li>
                <li class="button"><button onclick="getTilesInfo()">Reload Tiles</button></li>
                <li class="button">filename<input id="filename"/></li>
                <li class="button">merge<input type="checkbox" id="merge"></li>
                <li class="button">trim<input type="checkbox" id="trim"></li>
                <li class="button">sort<input type="checkbox" id="sort"></li>
                <li class="botton"><button onclick="filterPbfDownload()">Filter Pbf</button></li>
                
                
            </ul>
        </div>
        
        <div id="quadtreepanel" class="right body col scroll-y">
            <span class="icon close" onclick="closeQuadtree()"></span>
            <h4 id="quadtreehead">Quadtree</h4>
            <ul id="quadtreetable">
            </ul>
        </div>
        <div id="map" class="center body col"></div>
        
    </div>
    
    
    
    

    
	<script>

		var osmUrl='http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
        var osmAttrib='Map data © <a href="http://openstreetmap.org">OpenStreetMap</a> contributors';
        var osm = new L.TileLayer(osmUrl, {maxZoom: 18, attribution: osmAttrib});		

        var map = L.map('map', {center: [51.5,0], zoom: 15, layers: [osm]});
        var popup = L.popup();
        
        function rad(l) { return l*Math.PI/180.0; }
        function getTileURL(lat, lon, zoom) {
            var xtile = parseInt(Math.floor( (lon + 180) / 360 * (1<<zoom) ));
            var ytile = parseInt(Math.floor( (1 - Math.log(Math.tan(rad(lat)) + 1 / Math.cos(rad(lat))) / Math.PI) / 2 * (1<<zoom) ));
            return "" + zoom + "/" + xtile + "/" + ytile;
        }
		
        function onMapClick(e) {
			popup
				.setLatLng(e.latlng)
				.setContent("You clicked the map at " + e.latlng.toString() +" [tile: " + getTileURL(e.latlng.lat, e.latlng.lng, map.getZoom())+"]")
				.openOn(map);
		}
        
        
        
        
        
		map.on('click', onMapClick);
        
        var tilesOverlay = L.geoJson({},{
            style: function(f) { 
                if (f.properties && f.properties.layer == "parent") {
                    if (f.properties.Z == 0) {
                        return {color: 'yellow', fillOpacity: 0.1};
                    } else {
                        return {color: 'yellow', fillOpacity: 0};
                    }
                }
                if (f.properties && f.properties.layer == "object") {
                    return { color: 'green', fillOpacity: 0.1};
                }
                return { color: 'blue', fillOpacity: 0.1};
            },
            filter: function(f, l) {
                if (f.properties === undefined) { return false; }
                //if (f.properties.z < 8) { return false; }
                return true;
            },
            onEachFeature: function(f,l) {
                if (f.properties) {
                    l.on('click',function() {setTiles(f.properties.Str); } );
                }
            },
            });
        
        L.control.layers({"osm":osm}, {"quadtree": tilesOverlay}).addTo(map);

        
        var boundingGroup = L.featureGroup().addTo(map);
        var btype = "none";
        /*var drawControl = new L.Control.Draw({
            edit: {
                featureGroup: boundingGroup
            }
        });
        map.addControl(drawControl);*/
        
        //map.on("mouseover", function(e) { console.log("bg clicked"); setBounds(); });
        

	</script>
    
    
    
    <script>
        var tiles;
        
        $(document).ready(function() {
            getTilesInfo()
            $("#top").change(updateBbox)
            $("#left").change(updateBbox)
            $("#right").change(updateBbox)
            $("#bottom").change(updateBbox)
        });
        
        function getVal(ele) {
            var ans = parseFloat($("#"+ele).val());
            if (ans==NaN) {
                throw 0;
            }
            return ans
        }
            
        
        function updateBbox() {
            try {
                var nb = [ [getVal("top"),getVal("left")],[getVal("bottom"),getVal("right")]];
                console.log("updateBbox()=>",nb);
                setBbox(nb);
                map.setBounds(nb);
            } catch (e) {
                console.log("updateBbox failed?");
            }
        }
        
        function bboxData() {
            var data = {};
            if (btype == "bbox") {
                var nb = boundingGroup.getBounds();
                data.minlon = nb._southWest.lng;
                data.minlat = nb._southWest.lat;
                data.maxlon = nb._northEast.lng;
                data.maxlat = nb._northEast.lat;
            } else if (btype == "poly") {
                data.lon = [];
                data.lat = [];
                var cc = [];
                $.each(boundingGroup._layers, function(i,c) {
                    cc = c.getLatLngs();
                    //console.log(i+"=>"+cc+" ["+cc.length+"]");
                });
            
                $.each(cc, function(i,c) {
                    data.lon.push(c.lng);
                    data.lat.push(c.lat);
                });
            }
            
            data["trim"] = $("#trim").prop("checked");
            data["merge"] = $("#merge").prop("checked");
            data["sort"] = $("#sort").prop("checked");
            data["filename"] = $("#filename").val();
            return data;
        };
        
        function getTilesInfo() {
            var url = "info";
            var data = bboxData();
            
            console.log(url,data);
            $.ajax({
                url: url,
                data: data,
                type: "GET",
                dataType: "json",
                success: function( json ) {
                    tiles = json.qts;
                    setTiles("");
                }
            });
        }
        
        function filterPbfDownload() {
            var url = "filter";
            var data = bboxData();
            
            
            console.log("filterPbfDownload",url,data);
            $.fileDownload(url, {
                data: data,
                failCallback: function (html, url) {
                    
                    alert('Your file download just failed for this URL:' + url + '\r\n' +
                        'Here was the resulting error HTML: \r\n' + html
                    );
                }
            });
            
        }
        
        function setTiles(qtstr) {
            var currs = findTile(tiles, qtstr, []);
            
            var qtstable = $("#quadtreetable");
            qtstable.empty();
            
            tilesOverlay.clearLayers();
            var lc = currs.length-1;
            $.each(currs, function(i,t) {
                var ot = (i==lc ? "object" : "parent");
                makeElement(t,ot,qtstable,t.Str);
                addBox(t, ot);
            });
            
            //var l2 = $("<ul/>");
            $.each(currs[currs.length-1].Children, function(i,q) {
                makeElement(q,"child",qtstable,q.Str);
                addBox(q, "child");
            });
            //l2.appendTo(qtstable);
            
        }
        
        function makeElement(object, type, element, parent) {
            var e = $("<li/>");
            e.addClass("tileinfo "+type);
            var tt = object.Str+": "+object.Z+"/"+object.X+"/"+object.Y+" ["+object.Minx.toFixed(5)+","+object.Miny.toFixed(5)+","+object.Maxx.toFixed(5)+","+object.Maxy.toFixed(5)+"]";
            //e.prop("qt",qq.Str);
            if (parent!="NONE") {
                e.on("click",function() { setTiles(parent); } );
            }
            e.text(tt);
            e.appendTo(element);
        }
        
        function addBox(qq, lay) {
            var pp = {};
            pp.type="Feature";
            pp.geometry={};
            pp.geometry.type="Polygon";
            pp.geometry.coordinates=[[[qq.Minx,qq.Miny],[qq.Minx,qq.Maxy],[qq.Maxx,qq.Maxy],[qq.Maxx,qq.Miny],[qq.Minx,qq.Miny]]];
            
            pp.properties={};
            pp.properties.layer=lay;
            pp.properties.X=qq.X;
            pp.properties.Y=qq.Y;
            pp.properties.Z=qq.Z;
            pp.properties.Str=qq.Str;
            pp.properties.Parent=qq.Parent;
            tilesOverlay.addData(pp);
        }
            
        function findTile(parent, qtstr, ll) {
            
            ll.push(parent);
            if (parent.Str == qtstr) {
                return ll;
            }
            for (var i=0; i < parent.Children.length; ++i) {
                var cs = parent.Children[i].Str;
                if (qtstr.substr(0,cs.length)==cs) {
                    return findTile(parent.Children[i],qtstr,ll);
                }
            }
            return ll;
        }
        
        function getMapBox(makenew) {
            
            var bb = map.getBounds();
            
            var tb = boundingGroup.getBounds();
            if (makenew && (tb._northEast!==undefined)) {
                if (tb.intersects(bb)) {
                    return [[tb._northEast.lat,tb._northEast.lng],[tb._southWest.lat,tb._southWest.lng]];
                }
            }
            
            
            var w = bb._northEast.lng - bb._southWest.lng;
            var h = bb._northEast.lat - bb._southWest.lat;
            
            var nb = [
                [bb._northEast.lat - 0.1*h, bb._northEast.lng - 0.1*w],
                [bb._southWest.lat + 0.1*h, bb._southWest.lng + 0.1*w]
            ];
            return nb;
            
        }
        function makeBbox() {
            var nb = getMapBox(btype!="bbox");
            setBbox(nb);
        }
        
        function setBbox(nb) {
            boundingGroup.clearLayers();
            
            var r = L.rectangle(nb,{color: '#000', weight: 3, editable: true});
            r.on("edit", function(e) { setBounds(); });
            r.addTo(boundingGroup);
            setBounds();
            btype = "bbox";
            setPolyCoords();
        }
        
        function clearBounds() {
            boundingGroup.clearLayers();
            btype = "none";
            setPolyCoords();
            setBounds();
        }
        
        function makePolygon() {
            var nb = getMapBox(btype!="poly");
            var a = nb[0][0];
            var b = nb[0][1];
            var c = nb[1][0];
            var d = nb[1][1];
            
            var cc = [[a,b],[a,d],[c,d],[c,b],[a,b]];
            //console.log(cc);
            boundingGroup.clearLayers();
            var  p = L.polygon(cc,{color: '#000', weight: 3, editable: true, allowIntersection: false});
            p.on("edit", function(e) { setBounds(); setPolyCoords();});
            p.addTo(boundingGroup);
            
            
            btype="poly";
            setBounds();
            setPolyCoords();
        }
        
        function setBounds() {
            var nb = boundingGroup.getBounds();
            if (nb._northEast===undefined) {
                $("#top").val("");
                $("#left").val("");
                $("#right").val("");
                $("#bottom").val("");
                return;
            }
            
            $("#top").val(nb._northEast.lat.toFixed(5));
            $("#right").val(nb._northEast.lng.toFixed(5));
            $("#left").val(nb._southWest.lng.toFixed(5));
            $("#bottom").val(nb._southWest.lat.toFixed(5));
        }
        
        function setPolyCoords() {
            $("#polycoords").html("");
            if (btype != "poly") { return; }
            var cc = [];
            $.each(boundingGroup._layers, function(i,c) {
                cc = c.getLatLngs();
                //console.log(i+"=>"+cc+" ["+cc.length+"]");
            });
            
            
            var pt = "<table>";
            if (cc.length > 0) {
                
                $.each(cc, function(i,c) {
                    //if (i>0) { pt += ", " };
                    pt += "<tr><td>"+c.lng.toFixed(5)+"</td><td>"+c.lat.toFixed(5)+"</td></tr>";
                });
                
            }
            pt += "</table>";
            $("#polycoords").html(pt);
        }
        
        function closeQuadtree() {
            if ($("#quadtreepanel").css("width")!=="400px") {
                $("#quadtreepanel").css("width", "400px");
                $("#quadtreehead").text("Quadtree");
                
            } else {
                $("#quadtreepanel").css("width", "50px");
                $("#quadtreehead").text("");
                $("#quadtreetable").text("");
            }
        }
        
    </script>
    
</body>


</html>
`))
}