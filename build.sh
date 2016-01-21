if [ -z ${GOBIN+x} ]
    then export GOBIN=$HOME/.local/bin;
fi

go run make_static.go

echo "install in $GOBIN"
go clean github.com/jharris2268/osmquadtree
go install github.com/jharris2268/osmquadtree
#rm -R ${GOPATH}/pkg/linux_amd64/github.com/jharris2268/osmquadtree
go install osmquadtree-prepare.go
go install osmquadtree-initial.go
go install osmquadtree-update.go
go install osmquadtree-geometry.go
go install osmquadtree-filter.go static.go
go install osmquadtree-postgis.go
go install osmquadtree-rebase.go
go install osmquadtree-geometry-features.go
go install osmquadtree-postgis-alt.go
