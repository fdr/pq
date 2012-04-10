package pq

import (
	"fmt"
	"os"
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
	s, err := c.SimpleQuery(
		"SELECT 0; " +
			"SELECT generate_series(1, 3); " +
			"SELECT 'hello', 'goodbye';")

	if err != nil {
		t.Fatal(err)
	}

	rows := make([]row, 0, 2)

	for {
		emitted, err := s.pgBusyNext()
		if err != nil {
			t.Fatal(err)
		}

		switch conc := emitted.(type) {
		case result:
		case row:
			rowCopy := make(row, len(conc))

			copy(rowCopy, conc)
			rows = append(rows, rowCopy)
		case nil:
			goto Escape
		default:
			t.Fatalf("Unexpected emission: %q", emitted)
		}

		if emitted == nil {
			break
		}
	}

Escape:
	results := fmt.Sprintf("%q", rows)
	expected := `[["0"] ["1"] ["2"] ["3"] ["hello" "goodbye"]]`

	if results != expected {
		t.Fatalf("\nGot:\t\t%v\nExpected:\t%v", results, expected)
	}
}
