Ghostgres
======

[![GoDoc](https://godoc.org/github.com/surullabs/ghostgres?status.png)](https://godoc.org/github.com/surullabs/ghostgres) [![Build Status](https://drone.io/github.com/surullabs/ghostgres/status.png)](https://drone.io/github.com/surullabs/ghostgres/latest) [![Coverage Status](https://coveralls.io/repos/surullabs/ghostgres/badge.png?branch=master)](https://coveralls.io/r/surullabs/ghostgres?branch=master)

Ghostgres helps you start and control fresh PostgreSQL clusters very fast.
On a 2.000 GHz Intel i7-2630QM CPU machine (non-SSD) cloning a cluster took < 100ms.
However, your mileage may vary.

Please note that this is a work in progress and the API and documentation are in need
of some cleanup. The API will likely change in the future.

Running a unit test while talking to a database can be tricky and
sometimes requires a fair amount of setup to ensure the right
database is installed and configured on developer machines,
continuous build machines and other places where the tests run.

Ghostgres helps solve this by providing utilities to create a
standalone PostgreSQL cluster. It comes with the following
features for ease of use

   * Initialization of a fresh PostgreSQL cluster at a chosen location using initdb.
   * Cloning an existing installation for fast test startup times.
   * Controlling configuration of the cluster at initialization time.

The easiest way to use Ghostgres is to allow it to create a default template
for your version of postgres. This is done in the Ghostgres package directory
and is never modified. All future clusters are created by copying this directory
tree to a new location and starting a postgres server from there. This takes 10s of
ms vs 2 to 3 seconds to run initdb.

## Quick Start

	// Fetch the package
	go get -t github.com/surullabs/ghostgres

	// Run tests and create a default postgres cluster that will be used
	// as a template for future clusters. This will take a while since
	// it creates a number of postgres clusters from scratch. However, it also
	// reports the time taken to clone a single cluster which is what you will see
	// in practice.
	go test github.com/surullabs/ghostgres --gocheck.vv --ghostgres_pg_bin_dir=<path_to_your_postgres_bin_dir>


In your test code you can now use (with appropriate error checks)

	// Create a cloned cluster from the default template in a temporary directory
	cluster, err := ghostgres.FromDefault("")
	if err != nil {
		// fail
	}
	// Start the postgres server
	err = cluster.Start() // Handle error
	// Remember to stop it! This will delete the temporary directory.
	defer cluster.Stop()

	// Connect to the running postgres server through a unix socket.
	var connStr string
	connStr, err = cluster.TestConnectString() // Handle error
	db, err := sql.Open("postgres", fmt.Sprintf("%s dbname=postgres", connStr))

## Documentation and Examples

Please consult the package [GoDoc](https://godoc.org/github.com/surullabs/ghostgres)
 for detailed documentation.

## Licensing and Usage

Ghostgres is licensed under a 3-Clause BSD license. Please consult the
LICENSE file for details.

We also ask that you please file bugs and enhancement requests if you run
into any problems. In additon, we're always happy to accept pull requests!
If you do find this useful please share it with others who might also find
it useful. The more users we have the better the software becomes.

