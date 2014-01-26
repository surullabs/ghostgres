Ghostgres
======

Ghostgres helps you manage PostgreSQL clusters in Go unit tests.

Running a unit test while talking to a database can be tricky and
sometimes requires a fair amount of setup to ensure the right
database is installed and configured on developer machines,
continuous build machines and other places where the tests run.

Ghostgres helps solve this by providing utilities to create a
standalone PostgreSQL cluster. It comes with the following
features for ease of use

   * Initialization of a fresh PostgreSQL cluster at a chosen location.
   * Cloning an existing installation for fast test startup times.
   * Controlling configuration of the cluster at initialization time.

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

[![GoDoc](https://godoc.org/github.com/surullabs/ghostgres?status.png)](https://godoc.org/github.com/surullabs/ghostgres)
