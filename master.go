// Copyright 2014, Surul Software Labs GmbH
// All rights reserved.

package ghostgres

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/surullabs/fault"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
)

const templateDir = "testdata/template"

var pgBinDir = flag.String("ghostgres_pg_bin_dir", "", "Directory containing PostgreSQL binaries")
var defaultName = flag.String("ghostgres_template", "default", "The value for the default template database")

func postgresBinary() string { return filepath.Join(*pgBinDir, "postgres") }

func parseVersion(output string) (version string) {
	version = regexp.MustCompile("[0-9]+\\.[0-9]+\\.[0-9]+").FindString(output)
	fault.Check(version != "", fmt.Sprintf("failed to parse postgres version from %s", output))
	return
}

func postgresVersion() (version string) {
	return parseVersion(string(fault.CheckReturn(exec.Command(postgresBinary(), "--version").Output()).([]byte)))
}

type ghostgresTemplate string

var gopathFn = func() string { return os.Getenv("GOPATH") }

func newTemplate(root, name string) ghostgresTemplate {
	if root == DefaultTemplateDir {
		gopath := gopathFn()
		fault.Check(gopath != "", "GOPATH is not set. Unable to locate templates")
		// Use reflection to determine the package path so we're safe from package
		// relocations.
		pkgPath := filepath.Join(gopath, filepath.Join("src", reflect.TypeOf(PostgresCluster{}).PkgPath()))
		root = filepath.Join(pkgPath, templateDir)
	}
	if name == DefaultTemplate {
		name = *defaultName
	}
	return ghostgresTemplate(filepath.Join(root, filepath.Join(name, filepath.Join(postgresVersion()))))
}

func (t ghostgresTemplate) path() string   { return string(t) }
func (t ghostgresTemplate) data() string   { return filepath.Join(t.path(), "data") }
func (t ghostgresTemplate) config() string { return filepath.Join(t.path(), "ghostgres.json") }
func (t ghostgresTemplate) exists() bool {
	_, err := os.Stat(t.config())
	return err == nil
}
func (t ghostgresTemplate) clone(cloneDir string) *PostgresCluster {
	cluster := PostgresCluster{}
	fault.CheckError(json.Unmarshal(fault.CheckReturn(ioutil.ReadFile(t.config())).([]byte), &cluster))
	var onStop func()
	if cloneDir == "" {
		tempDir := fault.CheckReturn(ioutil.TempDir("", "ghostgres_clone")).(string)
		cloneDir = filepath.Join(tempDir, "clone")
		onStop = func() { os.RemoveAll(tempDir) }
	}
	cloned := fault.CheckReturn(cluster.Clone(cloneDir)).(*PostgresCluster)
	cloned.onStop = onStop
	return cloned
}
func (t ghostgresTemplate) createFrom(c *PostgresCluster) (err error) {
	fault.Check(!c.Running(), "cannot create a template from a running cluster")
	fault.CheckError(os.MkdirAll(t.path(), 0700))
	clone := fault.CheckReturn(c.Clone(t.data())).(*PostgresCluster)
	marshalled := fault.CheckReturn(json.MarshalIndent(clone, "", "  ")).([]byte)
	return ioutil.WriteFile(t.config(), marshalled, 0600)
}

// DefaultTemplateDir is a convenience value used to refer to the
// installed location of the ghostgres package. It is to be used as the
// root location if you would like to have ghostgres manage all template
// copies.
const DefaultTemplateDir = ""

// DefaultTemplate is a convenience value used to refer to a default
// template. If used the value of the --ghostgres_template flag will
// be used as the template name.
const DefaultTemplate = ""

// FromDefault is equivalent to FromTemplate(DefaultTemplateDir, DefaultTemplate, dest)
func FromDefault(dest string) (p *PostgresCluster, err error) {
	return FromTemplate(DefaultTemplateDir, DefaultTemplate, dest)
}

// FromTemplate will attempt to clone a cluster from a template located at
//
//	%dir%/%name%/%pg_version%/
//
// where dir and name have the same behaviour as in Freeze(dir,name).
//
// If the defaults don't exist an error will be returned. Please call
// Freeze(dir, name) first before calling FromTemplate.
//
// If dest is empty a temporary directory is created for the clone and will
// be deleted when Stop() is called on the cluster.
func FromTemplate(dir, name, dest string) (p *PostgresCluster, err error) {
	defer func() { fault.Recover(&err, recover()) }()
	return newTemplate(dir, name).clone(dest), nil
}

// Freeze will save a template to
//
//	%dir%/%name%/%pg_version%/
//
// where
//	%dir%		directory into which to freeze. This will
//			create a copy of the cluster into %dir%/data
//			If %dir% is empty <path_to_ghostgres>/testdata/template is used.
//	%name%		is the value of the parameter 'name'. If empty the value of
//			the ghostgres_template flag is used.
//	%pg_version%	is the result of calling PostgresVersion()
//
// If a frozen template exists it will return an error
func (cluster *PostgresCluster) Freeze(dir, name string) (err error) {
	defer func() { fault.Recover(&err, recover()) }()
	return newTemplate(dir, name).createFrom(cluster)
}

// Delete will delete a saved template configuration. dir and name
// have the same behaviour as in Freeze.
func Delete(dir, name string) (err error) {
	defer func() { fault.Recover(&err, recover()) }()
	return os.RemoveAll(newTemplate(dir, name).path())
}
