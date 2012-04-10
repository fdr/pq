package pq

import (
	"os"
	"fmt"
	"testing"
)

func openPgConn(t *testing.T) *conn {
	datname := os.Getenv("PGDATABASE")
	sslmode := os.Getenv("PGSSLMODE")

	if datname == "" {
		os.Setenv("PGDATABASE", "pqgotest")
	}

	if sslmode == "" {
		os.Setenv("PGSSLMODE", "disable")
	}

	dconn, err := Open("")
	if err != nil {
		t.Fatal(err)
	}

	return dconn.(*conn)
}

func TestSimple(t *testing.T) {
	c := openPgConn(t)
	s, err := c.SimpleQuery("SELECT 1; SELECT 2, 3;")
	if err != nil {
		t.Fatal(err)
	}

	for {
		emitted, err := s.pgBusyNext()
		if err != nil {
			t.Fatal(err)
		}

		switch emitted.(type) {
		default:
			fmt.Printf("%c %#v %#v\n", emitted, t, s)
		}

		if emitted == nil { break }
	}

	t.Fatal("nope")
}
