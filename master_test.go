// Copyright 2014, Surul Software Labs GmbH
// All rights reserved.

package ghostgres

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCreateDefaults(t *testing.T) {
	defer func() {
		if err := handlePanic(nil, recover()); err != nil {
			fmt.Println(err)
		}
	}()
	defaultTpl := newTemplate(DefaultTemplateDir, DefaultTemplate)
	version := postgresVersion()
	if defaultTpl.exists() {
		fmt.Println("Default template exists for version", version, "at", defaultTpl.path())
	} else {

		fmt.Println("Creating default template for version", version, "at", defaultTpl.path())
		tempDir := checkErr(ioutil.TempDir("", "ghostgres_default")).(string)
		defer func() { os.RemoveAll(tempDir) }()
		cluster := &PostgresCluster{
			Config:   TestConfigWithLogging,
			BinDir:   *pgBinDir,
			DataDir:  tempDir,
			FailWith: t.Fatal,
			Password: "ghostgres",
		}
		checkErr(nil, cluster.Init())
		checkErr(nil, cluster.Freeze(DefaultTemplateDir, DefaultTemplate))

		fmt.Println("Created default template for version", version, "at", defaultTpl.path())
	}

	// Now test that we can clone it.
	before := time.Now()
	cloned := checkErr(FromDefault("")).(*PostgresCluster)
	atClone := time.Now()
	fmt.Printf("Cloning a new cluster takes %0.4f seconds\n", atClone.Sub(before).Seconds())

	checkErr(nil, cloned.Start())
	defer cloned.Stop()
	checkErr(nil, cloned.WaitTillRunning(1*time.Second))
	checkErr(os.Stat(filepath.Dir(cloned.DataDir)))

	str := fmt.Sprintf("%s dbname=postgres", cloned.TestConnectString())
	t.Log("Opening db connection", str)
	db := checkErr(sql.Open("postgres", str)).(*sql.DB)
	defer db.Close()

	t.Log("Running query")
	var count int
	checkErr(nil, db.QueryRow("SELECT count(*) FROM pg_database WHERE datistemplate = false;").Scan(&count))
	t.Log("Finished query")
	check(count == 1, "mismatched count")
	db.Close()

	checkErr(nil, cloned.Stop())
	_, err := os.Stat(filepath.Dir(cloned.DataDir))
	check(os.IsNotExist(err), "Directory not cleaned up")

}

func checkPanic(c *C, matchRe string, fn func()) {
	defer func() {
		if e := recover(); e == nil {
			c.Fatal("No panic occured")
		} else if _, isErr := e.(ghostgresPanic); isErr {
			c.Assert(fmt.Errorf("%v", e), ErrorMatches, matchRe)
		} else {
			c.Fatal(e)
		}
	}()
	fn()
}

func (s *PostgresSuite) TestUtilFailures(c *C) {
	checkPanic(c, ".*no such file or directory.*", func() {
		oldBinDir := *pgBinDir
		defer func() { *pgBinDir = oldBinDir }()
		*pgBinDir = c.MkDir()
		postgresVersion()
	})
	checkPanic(c, "failed to parse postgres version from blah", func() { parseVersion("blah") })
	checkPanic(c, "GOPATH is not set.*", func() {
		oldPath := gopathFn
		defer func() { gopathFn = oldPath }()
		gopathFn = func() string { return "" }
		newTemplate(DefaultTemplateDir, DefaultTemplate)
	})
}

func (s *PostgresSuite) TestTemplating(c *C) {
	c.Assert(
		filepath.Dir(newTemplate(DefaultTemplateDir, DefaultTemplate).path()),
		Equals,
		checkErr(filepath.Abs(filepath.Join(templateDir, *defaultName))).(string))
	cluster := initdb(c)
	freezeDir := c.MkDir()
	c.Assert(cluster.Freeze(freezeDir, "mytpl"), IsNil)
	cloneDest := filepath.Join(c.MkDir(), "clone")
	cloned, err := FromTemplate(freezeDir, "mytpl", cloneDest)
	c.Assert(err, IsNil)
	c.Assert(cloned, Not(IsNil))
	c.Assert(cloned.DataDir, Not(Equals), cluster.DataDir)
	cloned.FailWith = c.Fatal
	CheckCluster(cloned, c)
	Delete(freezeDir, "mytpl")
	cloneDest = filepath.Join(c.MkDir(), "clone")
	cloned, err = FromTemplate(freezeDir, "mytpl", cloneDest)
	c.Assert(err, ErrorMatches, ".*no such file.*")
}
