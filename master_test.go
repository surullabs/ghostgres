// Copyright 2014, Surul Software Labs GmbH
// All rights reserved.

package ghostgres

import (
	"database/sql"
	"fmt"
	"github.com/surullabs/fault"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCreateDefaults(t *testing.T) {
	defer func() {
		var err error
		if testcheck.Recover(&err); err != nil {
			t.Fatal(err)
		}
	}()

	var gerr error
	defer checkError(&gerr, t.Error)
	defer check.Recover(&gerr)

	defaultTpl := newTemplate(DefaultTemplateDir, DefaultTemplate)
	version := postgresVersion()
	if defaultTpl.exists() {
		fmt.Println("Default template exists for version", version, "at", defaultTpl.path())
	} else {

		fmt.Println("Creating default template for version", version, "at", defaultTpl.path())
		tempDir := testcheck.Return(ioutil.TempDir("", "ghostgres_default")).(string)
		defer func() { os.RemoveAll(tempDir) }()
		cluster := &PostgresCluster{
			Config:   TestConfigWithLogging,
			BinDir:   *pgBinDir,
			DataDir:  tempDir,
			Password: "ghostgres",
		}
		testcheck.Error(cluster.Init())
		testcheck.Error(cluster.Freeze(DefaultTemplateDir, DefaultTemplate))

		fmt.Println("Created default template for version", version, "at", defaultTpl.path())
	}

	// Now test that we can clone it.
	before := time.Now()
	cloned := testcheck.Return(FromDefault("")).(*PostgresCluster)
	atClone := time.Now()
	fmt.Printf("Cloning a new cluster takes %0.4f seconds\n", atClone.Sub(before).Seconds())

	testcheck.Error(cloned.Start())
	defer cloned.Stop()
	testcheck.Error(cloned.WaitTillServing(1 * time.Second))
	testcheck.Return(os.Stat(filepath.Dir(cloned.DataDir)))

	str := fmt.Sprintf("%s dbname=postgres", testcheck.Return(cloned.TestConnectString()))
	t.Log("Opening db connection", str)
	db := testcheck.Return(sql.Open("postgres", str)).(*sql.DB)
	defer db.Close()

	t.Log("Running query")
	var count int
	testcheck.Error(db.QueryRow("SELECT count(*) FROM pg_database WHERE datistemplate = false;").Scan(&count))
	t.Log("Finished query")
	testcheck.True(count == 1, "mismatched count")
	db.Close()

	testcheck.Error(cloned.Stop())
	_, err := os.Stat(filepath.Dir(cloned.DataDir))
	testcheck.True(os.IsNotExist(err), "Directory not cleaned up")

}

func checkPanic(c *C, matchRe string, fn func()) {
	defer func() {
		if e := recover(); e == nil {
			c.Fatal("No panic occured")
		} else if faults, isErr := e.(fault.Fault); isErr {
			c.Assert(faults.Cause(), ErrorMatches, matchRe)
		} else {
			panic(e)
			c.Fatal(e)
		}
	}()
	fn()
}

func (s *PostgresSuite) TestUtilFailures(c *C) {
	var gerr error
	defer checkError(&gerr, c.Error)
	defer check.Recover(&gerr)

	checkPanic(c, ".*no such file or directory.*", func() {
		oldBinDir := *pgBinDir
		defer func() { *pgBinDir = oldBinDir }()
		*pgBinDir = c.MkDir()
		postgresVersion()
	})
	checkPanic(c, ".*failed to parse postgres version from blah", func() { parseVersion("blah") })
	checkPanic(c, ".*GOPATH is not set.*", func() {
		oldPath := gopathFn
		defer func() { gopathFn = oldPath }()
		gopathFn = func() string { return "" }
		newTemplate(DefaultTemplateDir, DefaultTemplate)
	})
}

func checkError(errp *error, logFn func(...interface{})) {
	if *errp != nil {
		logFn(fault.VerboseTrace(*errp))
	}
}

func (s *PostgresSuite) TestTemplating(c *C) {
	var gerr error
	defer checkError(&gerr, c.Error)
	defer check.Recover(&gerr)

	c.Assert(
		filepath.Dir(newTemplate(DefaultTemplateDir, DefaultTemplate).path()),
		Equals,
		testcheck.Return(filepath.Abs(filepath.Join(templateDir, *defaultName))).(string))
	cluster := initdb(c)
	freezeDir := c.MkDir()
	c.Assert(cluster.Freeze(freezeDir, "mytpl"), IsNil)
	cloneDest := filepath.Join(c.MkDir(), "clone")
	cloned, err := FromTemplate(freezeDir, "mytpl", cloneDest)
	c.Assert(err, IsNil)
	c.Assert(cloned, Not(IsNil))
	c.Assert(cloned.DataDir, Not(Equals), cluster.DataDir)
	CheckCluster(cloned, c)
	Delete(freezeDir, "mytpl")
	cloneDest = filepath.Join(c.MkDir(), "clone")
	cloned, err = FromTemplate(freezeDir, "mytpl", cloneDest)
	c.Assert(err, ErrorMatches, ".*no such file.*")
}
