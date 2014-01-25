// Copyright 2014, Surul Software Labs GmbH
// All rights reserved.
//
// This provides a utility to start and control a PostgreSQL database.
// The expected usage is in tests where it allows for easy startup and
// shutdown of a database.
package postgres

import (
	"errors"
	"fmt"
	surulio "github.com/surullabs/goutil/io"
	surultpl "github.com/surullabs/goutil/template"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"text/template"
	"time"
)

var postgresqlConfTemplate = template.Must(template.New("postgresql.conf").Parse(`# Auto Generated PostgreSQL Configuration
{{range $opt := $.Config}}
{{$opt.Key}} = {{$opt.Value}} {{if $opt.Comment}} # {{$opt.Comment}} {{end}}
{{end}}`))

// PostgreSQL configuration option
type ConfigOpt struct {
	Key     string
	Value   string
	Comment string
}

type FailureHandler func(...interface{})

// Configuration for a PostgreSQL cluster
type PostgresCluster struct {
	// Key value pairs used to create a postgresql.conf file. They are
	// written out as
	// 	key = value # comment
	Config []ConfigOpt
	// Directory in which to initialize the cluster.
	DataDir string
	// A set of options to be used when creating the cluster. These
	// will be passed directly to initdb. A example would be
	//  {"--auth", "trust", ""}, {"--nosync", "", ""} to enable easy testing.
	// For more details on the command line flags see
	// http://www.postgresql.org/docs/9.3/static/app-initdb.html
	InitOpts []ConfigOpt
	// A set of options to be used when running the postgres server.
	RunOpts []ConfigOpt
	// Directory containing postgres binaries
	BinDir string
	// The password for the super user
	Password string
	// Convenience when writing tests. All exported functions will call this
	// failure handler if it is not nil.
	FailWith FailureHandler
	// The running postgres process
	proc *exec.Cmd
}

func makeArgs(opts []ConfigOpt) []string {
	args := make([]string, 0)
	for _, arg := range opts {
		args = append(args, arg.Key)
		if arg.Value != "" {
			args = append(args, arg.Value)
		}
	}
	return args
}

var tempDir surulio.TempDirExecer = &surulio.SafeTempDirExecer{}

func (p *PostgresCluster) checkError(err error) {
	if p.FailWith != nil && err != nil {
		p.FailWith(err)
	}
}

// Clones a previous postgres database by copying the entire directory
// This currently only works on systems which have a cp command. This
// will not work if the destination directory exists.
func (p *PostgresCluster) Clone(dest string) (c *PostgresCluster, err error) {
	defer func() { p.checkError(err) }()

	if p.Running() {
		err = errors.New("cannot clone a running cluster")
		return
	}

	if _, err = os.Stat(dest); err == nil {
		err = errors.New("Cannot clone into an existing directory")
		return
	} else if !os.IsNotExist(err) {
		return
	}

	var output []byte
	if output, err = exec.Command("cp", "-r", p.DataDir, dest).CombinedOutput(); err != nil {
		err = fmt.Errorf("%v: %s", err, string(output))
		return
	}
	cloned := *p
	cloned.DataDir = dest
	c = &cloned
	return
}

func (p *PostgresCluster) InitIfNeeded() (err error) {
	if !p.Initialized() {
		err = p.Init()
	}
	return
}

// This will run initdb to create the cluster in the specified
// directory.
func (p *PostgresCluster) Init() (err error) {
	defer func() { p.checkError(err) }()

	args := make([]ConfigOpt, len(p.InitOpts))
	copy(args, p.InitOpts)
	args = append(args, ConfigOpt{"--pgdata", p.DataDir, ""})

	err = tempDir.Exec("pg_init", func(dir string) error {
		passwordFile := filepath.Join(dir, "postgres_pass")
		if writeErr := ioutil.WriteFile(passwordFile, []byte(p.Password), 0600); writeErr != nil {
			return writeErr
		}

		args = append(args, ConfigOpt{"--pwfile", passwordFile, ""})
		initdb := exec.Command(filepath.Join(p.BinDir, "initdb"), makeArgs(args)...)
		if output, runErr := initdb.CombinedOutput(); runErr != nil {
			return fmt.Errorf("%v: %s", runErr, string(output))
		}
		return nil
	})
	if err != nil {
		return
	}

	// Now write out the postgresql.conf
	return surultpl.WriteFile(p.configFile(), postgresqlConfTemplate, p, 0600)
}

func (p *PostgresCluster) configFile() string { return filepath.Join(p.DataDir, "postgresql.conf") }

// Port attempts to parse a port from the provided config options
// and returns the parsed port or 5432 if there is a failure or no
// port is specified. If there is a failure the FailWith will
// be called.
func (p *PostgresCluster) Port() int {
	port := "5432"
	for _, opt := range p.Config {
		if opt.Key == "port" {
			port = opt.Value
			break
		}
	}
	if portVal, err := strconv.Atoi(port); err != nil {
		// The port is invalid, falling back to 5432
		p.checkError(err)
		return 5432
	} else {
		return portVal
	}
}

// SocketDir returns the location of the postgres unix socket directory
func (p *PostgresCluster) SocketDir() string { return p.DataDir }

// SocketFile returns the location of the postgres socket file
func (p *PostgresCluster) SocketFile() string {
	return filepath.Join(p.SocketDir(), fmt.Sprintf(".s.PGSQL.%d", p.Port()))
}

func (p *PostgresCluster) Initialized() bool {
	if exists, err := surulio.Exists(p.configFile()); exists && err == nil {
		return true
	}
	return false
}

func (p *PostgresCluster) WaitTillRunning(timeout time.Duration) (err error) {
	defer func() { p.checkError(err) }()
	if p.proc == nil {
		err = errors.New("The server has not been started")
	} else {
		err = surulio.WaitTillExists(p.SocketFile(), 10*time.Millisecond, timeout)
	}
	return
}

func (p *PostgresCluster) Running() bool {
	// TODO: Run the process in a separate goroutine and make this more robust.
	return p.proc != nil
}

func (p *PostgresCluster) Start() (err error) {
	defer func() { p.checkError(err) }()
	if !p.Initialized() {
		err = errors.New("The postgres cluster is not initialized. Please call Init()")
		return
	}

	if p.Running() {
		err = errors.New("The postgres cluster is already running")
		return
	}

	args := make([]ConfigOpt, len(p.RunOpts))
	copy(args, p.RunOpts)
	args = append(args, ConfigOpt{"-D", p.DataDir, ""})
	args = append(args, ConfigOpt{"-k", p.DataDir, ""})
	args = append(args, ConfigOpt{"-c", fmt.Sprintf("config_file=%s", p.configFile()), ""})
	p.proc = exec.Command(filepath.Join(p.BinDir, "postgres"), makeArgs(args)...)
	if err = p.proc.Start(); err != nil {
		p.proc = nil
		return
	}
	return
}

func (p *PostgresCluster) Wait() (err error) {
	defer func() { p.checkError(err) }()
	if !p.Running() {
		err = errors.New("The postgres cluster is not running")
		return
	}
	defer func() { p.proc = nil }()
	err = p.proc.Wait()
	return
}

func (p *PostgresCluster) Stop() (err error) {
	defer func() { p.checkError(err) }()
	if !p.Running() {
		return
	}
	p.proc.Process.Signal(syscall.SIGTERM)
	err = p.Wait()
	return
}
