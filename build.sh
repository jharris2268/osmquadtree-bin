if [ -z ${GOBIN+x} ]
    then export GOBIN=$HOME/.local/bin;
fi

go run make_static.go

echo "install in $GOBIN"
go install -a osmquadtree-prepare.go
go install -a osmquadtree-initial.go
go install -a osmquadtree-update.go
go install -a osmquadtree-geometry.go
go install -a osmquadtree-filter.go static.go
go install -a osmquadtree-postgis.go

