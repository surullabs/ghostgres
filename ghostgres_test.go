// Copyright 2014, Surul Software Labs GmbH
// All rights reserved.
//
package ghostgres

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	. "github.com/surullabs/goutil/testing"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

type PostgresSuite struct{}

var _ = Suite(&PostgresSuite{})

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) {
	TestingT(t)
}

func CheckCluster(cluster *PostgresCluster, c *C) {
	c.Assert(cluster.DataDir, HasFilesNamed, []string{"PG_VERSION", "pg_hba.conf"})
	c.Assert(filepath.Join(cluster.DataDir, "postgresql.conf"), FileMatches, "testdata/TestInit_postgresql.conf")

	c.Log("Starting cluster")
	c.Assert(cluster.Start(), IsNil)
	defer cluster.Stop()

	c.Log("Cluster started. Waiting for it to run")
	c.Assert(cluster.WaitTillRunning(1*time.Second), IsNil)

	c.Log("Opening db connection")
	db, err := sql.Open("postgres", fmt.Sprintf("sslmode=disable dbname=postgres host=%s port=%d", cluster.SocketDir(), cluster.Port()))
	c.Assert(err, IsNil)

	defer db.Close()

	c.Log("Running query")
	var count int
	c.Assert(db.QueryRow("SELECT count(*) FROM pg_database WHERE datistemplate = false;").Scan(&count), IsNil)
	c.Log("Finished query")
	c.Assert(count, Equals, 1)
	c.Log("Closing connection")
	c.Assert(db.Close(), IsNil)
	c.Log("Stopping cluster")
	c.Assert(cluster.Stop(), IsNil)
}

func testCluster(c *C) *PostgresCluster {
	return &PostgresCluster{
		Config: []ConfigOpt{
			{"port", "10000", "Different port for testing local sockets"},
			{"listen_addresses", "''", "Don't listen on TCP"},
			{"autovacuum", "off", "Don't run autovacuum"},
			{"fsync", "off", ""},
		},
		DataDir:  c.MkDir(),
		BinDir:   *pgBinDir,
		Password: "This is random",
		FailWith: c.Fatal,
	}
}

func initdb(c *C) *PostgresCluster {
	cluster := testCluster(c)
	c.Assert(cluster.Init(), IsNil)
	return cluster
}

func (s *PostgresSuite) TestBadPort(c *C) {
	cluster := testCluster(c)
	cluster.Config = []ConfigOpt{{"port", "this is a bad port", ""}}
	var err error
	cluster.FailWith = func(args ...interface{}) { err = args[0].(error) }
	c.Assert(cluster.Port(), Equals, 5432)
	c.Assert(err, ErrorMatches, ".*this is a bad port.*")
}

func (s *PostgresSuite) TestInit(c *C) {
	CheckCluster(initdb(c), c)
}

func (s *PostgresSuite) TestClone(c *C) {
	cluster := initdb(c)
	cloned, err := cluster.Clone(filepath.Join(c.MkDir(), "cloned"))
	c.Assert(err, IsNil)
	c.Assert(cloned.DataDir, Not(Equals), cluster.DataDir)
	CheckCluster(cloned, c)
}

func (s *PostgresSuite) TestInitIfNeeded(c *C) {
	for _, cluster := range []*PostgresCluster{initdb(c), testCluster(c)} {
		c.Assert(cluster.InitIfNeeded(), IsNil)
		CheckCluster(cluster, c)
	}
}

func checkFailure(c *C, cluster *PostgresCluster, fn func() error) {
	var failed error
	cluster.FailWith = func(args ...interface{}) {
		c.Assert(len(args), Equals, 1)
		c.Assert(args[0], NotNil)
		failed = args[0].(error)
	}
	expected := fn()
	c.Assert(expected, NotNil)
	c.Assert(failed, Equals, expected)

}

func (s *PostgresSuite) TestFailures(c *C) {
	cluster := testCluster(c)
	checkFailure(c, cluster, cluster.Start)

	cluster = initdb(c)
	defer cluster.Stop()

	checkFailure(c, cluster, cluster.Init)

	cloner := func(dir string) func() error {
		return func() error {
			_, err := cluster.Clone(dir)
			return err
		}
	}

	checkFailure(c, cluster, cloner(c.MkDir()))
	checkFailure(c, cluster, cloner(cluster.DataDir))
	checkFailure(c, cluster, cluster.Wait)
	checkFailure(c, cluster, func() error { return cluster.WaitTillRunning(10) })

	origOpts := cluster.RunOpts
	cluster.RunOpts = []ConfigOpt{{Key: "--fake_flag"}}
	c.Assert(cluster.Start(), IsNil)
	checkFailure(c, cluster, cluster.Wait)
	cluster.RunOpts = origOpts

	origBin := cluster.BinDir
	cluster.BinDir = c.MkDir()
	checkFailure(c, cluster, cluster.Start)
	cluster.BinDir = origBin

	c.Assert(cluster.Start(), IsNil)

	checkFailure(c, cluster, cluster.Start)
	checkFailure(c, cluster, cloner(filepath.Join(c.MkDir(), "cloned")))

	cluster.proc.Process.Signal(syscall.SIGINT)
	checkFailure(c, cluster, cluster.Stop)

	c.Assert(cluster.Start(), IsNil)

	cluster.proc.Process.Signal(syscall.SIGINT)
	checkFailure(c, cluster, cluster.Wait)
}

func Example() {
	// Using a postgres cluster with test defaults in a temporary directory

	tempDir, err := ioutil.TempDir("", "ghostgres")
	if err != nil {
		log.Fatal(err)
		return
	}
	defer func() { os.RemoveAll(tempDir) }()

	// A postgres cluster which will be created in tempDir and use binaries
	// from /usr/lib/postgresql/9.3/bin. log.Fatal will be run if any errors
	// occur on any  of the exported methods of ghostgres.
	// This can also be an instance of testing.T.Fatal to automatically abort
	// tests on error
	master := &PostgresCluster{
		Config:   TestConfig,
		DataDir:  tempDir,
		BinDir:   "/usr/lib/postgresql/9.3/bin",
		FailWith: log.Fatal,
	}

	// Initialize the cluster
	master.Init()
	master.Start()
	defer master.Stop()

	// Now use the database
	db, err := sql.Open("postgres", fmt.Sprintf("sslmode=disable dbname=postgres host=%s port=%d", master.SocketDir(), master.Port()))
	if err != nil {
		log.Fatal(err)
		return
	}
	defer db.Close()

	master.Stop()
}

func Example_cloning() {
	// Using a postgres cluster with test defaults that is cloned from a previously known
	// location.

	// The temporary directory will be used for the clone
	tempDir, err := ioutil.TempDir("", "ghostgres")
	if err != nil {
		log.Fatal(err)
		return
	}
	defer func() { os.RemoveAll(tempDir) }()

	// A postgres cluster which will be loaded from testdata/templatedb and use binaries
	// from /usr/lib/postgresql/9.3/bin. log.Fatal will be run if any errors
	// occur on any  of the exported methods of ghostgres.
	// This can also be an instance of testing.T.Fatal to automatically abort
	// tests on error
	master := &PostgresCluster{
		Config:   TestConfig,
		DataDir:  "testdata/templatedb",
		BinDir:   "/usr/lib/postgresql/9.3/bin",
		FailWith: log.Fatal,
	}

	// Initialize the cluster if needed. This allows you to create a template
	// easily. You can then choose to store the template in version control
	// but be warned that it takes up to 33 MB.
	master.InitIfNeeded()

	// Create a clone which we will use for tests.
	clone, _ := master.Clone(tempDir)

	defer clone.Stop()

	// Now use the database
	db, err := sql.Open("postgres", fmt.Sprintf("sslmode=disable dbname=postgres host=%s port=%d", clone.SocketDir(), clone.Port()))
	if err != nil {
		log.Fatal(err)
		return
	}
	defer db.Close()

	clone.Stop()
}
