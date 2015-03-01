if [ -z ${GOBIN+x} ]
    then export GOBIN=$HOME/.local/bin;
fi
echo "install in $GOBIN"
go install gobin/osmquadtree-prepare.go
go install gobin/osmquadtree-initial.go
go install gobin/osmquadtree-update.go
go install gobin/osmquadtree-geometry.go
go install gobin/osmquadtree-filter.go gobin/static.go
go install gobin/osmquadtree-postgis.go

