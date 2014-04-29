// Copyright 2014, Surul Software Labs GmbH
// All rights reserved.
//
package ghostgres

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/surullabs/fault"
	. "github.com/surullabs/goutil/testing"
	"io/ioutil"
	. "launchpad.net/gocheck"
	"log"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

var testcheck = fault.NewChecker().SetFaulter(&fault.DebugFaulter{})

type PostgresSuite struct{}

var _ = Suite(&PostgresSuite{})

// Hook up gocheck into the "go test" runner.
func TestGocheck(t *testing.T) {
	TestingT(t)
}

var postgresGoldenFmt = `# Auto Generated PostgreSQL Configuration

port = %d  # Different port for testing local sockets 

listen_addresses = ''  # Don't listen on TCP 

autovacuum = off  # Don't run autovacuum 

fsync = off 
`

func CheckCluster(cluster *PostgresCluster, c *C) {
	c.Assert(cluster.DataDir, HasFilesNamed, []string{"PG_VERSION", "pg_hba.conf"})
	port, err := cluster.Port()
	c.Assert(err, IsNil)
	expected := fmt.Sprintf(postgresGoldenFmt, port)
	cfgData, err := ioutil.ReadFile(filepath.Join(cluster.DataDir, "postgresql.conf"))
	c.Assert(err, IsNil)
	c.Assert(string(cfgData), Equals, expected)

	c.Log("Starting cluster")
	c.Assert(cluster.Start(), IsNil)
	defer cluster.Stop()

	c.Log("Cluster started. Waiting for it to run")
	c.Assert(cluster.WaitTillServing(1*time.Second), IsNil)

	c.Log("Opening db connection")
	db, err := sql.Open("postgres", fmt.Sprintf("%s dbname=postgres", testcheck.Return(cluster.TestConnectString()).(string)))
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

func getUnusedPort(c *C) int {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		c.Fatal("failed to detect unused port ", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func testCluster(c *C) *PostgresCluster {
	return &PostgresCluster{
		Config: []ConfigOpt{
			{"port", fmt.Sprintf("%d", getUnusedPort(c)), "Different port for testing local sockets"},
			{"listen_addresses", "''", "Don't listen on TCP"},
			{"autovacuum", "off", "Don't run autovacuum"},
			{"fsync", "off", ""},
		},
		DataDir:  c.MkDir(),
		BinDir:   *pgBinDir,
		Password: "This is random",
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
	_, err := cluster.Port()
	c.Assert(err, ErrorMatches, ".*this is a bad port.*")
}

func (s *PostgresSuite) TestInit(c *C) {
	CheckCluster(initdb(c), c)
}

// Note: This test depends a bit on timing and so could be flaky.
func (s *PostgresSuite) TestStopTerminated(c *C) {
	cluster := initdb(c)
	// Start and Stop immediately. If postgres is killed before it is
	// properly started it will return a signal: terminated error.
	c.Assert(cluster.Start(), IsNil)
	c.Assert(cluster.Stop(), IsNil)
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

func checkFailure(c *C, cluster *PostgresCluster, fn func() error, matches string) {
	expected := fn()
	c.Assert(expected, ErrorMatches, matches)

}

func (s *PostgresSuite) TestFailures(c *C) {
	cluster := testCluster(c)
	cloner := func(dir string) func() error {
		return func() error {
			_, err := cluster.Clone(dir)
			return err
		}
	}

	checkFailure(c, cluster, cluster.Start, ".*postgres cluster not initialized")
	checkFailure(c, cluster, cloner(filepath.Join(c.MkDir(), "clone")), ".*must be initialized.*")

	cluster = initdb(c)
	defer cluster.Stop()

	checkFailure(c, cluster, cluster.Init, ".*postgres cluster already initialized")

	checkFailure(c, cluster, cloner(c.MkDir()), ".*cannot clone into an existing directory")
	checkFailure(c, cluster, cluster.Wait, ".*postgres cluster not running")
	checkFailure(c, cluster, func() error { return cluster.WaitTillServing(10) }, ".*server has not been started")

	origOpts := cluster.RunOpts
	cluster.RunOpts = []ConfigOpt{{Key: "--fake_flag"}}
	c.Assert(cluster.Start(), IsNil)
	checkFailure(c, cluster, cluster.Wait, "exit status 1")
	cluster.RunOpts = origOpts

	origBin := cluster.BinDir
	cluster.BinDir = c.MkDir()
	checkFailure(c, cluster, cluster.Start, ".*no such file or directory.*")
	cluster.BinDir = origBin

	c.Assert(cluster.Start(), IsNil)

	checkFailure(c, cluster, cluster.Start, ".*already running.*")
	checkFailure(c, cluster, cloner(filepath.Join(c.MkDir(), "cloned")), ".*cannot clone a running cluster")

	cluster.proc.Process.Signal(syscall.SIGINT)
	checkFailure(c, cluster, cluster.Stop, ".*signal: interrupt")

	c.Assert(cluster.Start(), IsNil)

	cluster.proc.Process.Signal(syscall.SIGINT)
	checkFailure(c, cluster, cluster.Wait, ".*signal: interrupt")
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
		Config:  TestConfig,
		DataDir: tempDir,
		BinDir:  "/usr/lib/postgresql/9.3/bin",
	}

	// Initialize the cluster
	master.Init()
	master.Start()
	defer master.Stop()

	// Now use the database
	db, err := sql.Open("postgres", fmt.Sprintf("%s dbname=postgres", testcheck.Return(master.TestConnectString()).(string)))
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
		Config:  TestConfig,
		DataDir: "testdata/templatedb",
		BinDir:  "/usr/lib/postgresql/9.3/bin",
	}

	// Initialize the cluster if needed. This allows you to create a template
	// easily. You can then choose to store the template in version control
	// but be warned that it takes up to 33 MB.
	master.InitIfNeeded()

	// Create a clone which we will use for tests.
	clone, _ := master.Clone(tempDir)

	defer clone.Stop()

	// Now use the database
	db, err := sql.Open("postgres", fmt.Sprintf("%s dbname=postgres", testcheck.Return(clone.TestConnectString())))
	if err != nil {
		log.Fatal(err)
		return
	}
	defer db.Close()

	clone.Stop()
}
