package pq

import (
	"fmt"
	"os"
	"testing"
)

func openNewFebeContext(t *testing.T) *febeContext {
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

	return newFebeContext(dconn.(*conn))
}

func bufferAndCheckRows(t *testing.T, s *febeContext, expected string) {

	rows := make([]row, 0, 3)

	if s.state != PQ_STATE_BUSY {
		t.Fatalf("expected to be in the busy state, "+
			"but instead was in %q", s.state)
	}

Loop:
	for {
		state, emitted, err := s.pqNext()
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
			if state != PQ_STATE_IDLE {
				t.Fatal("expected connection to be idle "+
					"with nil emission, but it wasn't %q", s)
			}
			break Loop
		default:
			t.Fatalf("Unexpected emission: %q", emitted)
		}
	}

	if results := fmt.Sprintf("%q", rows); results != expected {
		t.Fatalf("\nGot:\t\t%v\nExpected:\t%v", results, expected)
	}
}

func TestSimpleSingleStatement(t *testing.T) {
	cxt := openNewFebeContext(t)
	err := cxt.SimpleQuery("SELECT 0;")
	if err != nil {
		t.Fatal(err)
	}

	bufferAndCheckRows(t, cxt, `[["0"]]`)
}

func TestSimpleMultiStatement(t *testing.T) {
	cxt := openNewFebeContext(t)
	err := cxt.SimpleQuery(`SELECT 0;
SELECT generate_series(1, 3);
SELECT 'hello', 'goodbye';`)
	if err != nil {
		t.Fatal(err)
	}

	bufferAndCheckRows(t, cxt,
		`[["0"] ["1"] ["2"] ["3"] ["hello" "goodbye"]]`)

}

func TestEmptyQuery(t *testing.T) {
	cxt := openNewFebeContext(t)
	err := cxt.SimpleQuery("")
	if err != nil {
		t.Fatal(err)
	}

	bufferAndCheckRows(t, cxt, `[]`)
}

func TestCopyOut(t *testing.T) {
	cxt := openNewFebeContext(t)
	err := cxt.SimpleQuery(
		`COPY (SELECT generate_series(1, 5)) TO STDOUT;`)
	if err != nil {
		t.Fatal(err)
	}

	copyOutRows := make([]copyOutData, 0, 5)

Loop:
	for {
		_, emitted, err := cxt.pqNext()
		if err != nil {
			t.Fatal(err)
		}

		switch typ := emitted.(type) {
		case result:
		case row:
			t.Fatal("expect no row messages")
		case copyOutData:
			copyOutRows = append(copyOutRows, typ)
		case nil:
			break Loop
		default:
			t.Fatalf("Unexpected emission: %q", emitted)
		}
	}

	t.FailNow()
}
