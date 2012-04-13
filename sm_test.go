package pq

import (
	"bytes"
	"fmt"
	"os"
	"runtime/debug"
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

	quickCompare(t, fmt.Sprintf("%q", rows), expected)
}

func quickCompare(t *testing.T, results, expected string) {
	if results != expected {
		debug.PrintStack()
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

func mustWaitForReady(t *testing.T, cxt *febeContext) {
	for {
		_, emit, err := cxt.pqNext()

		if err != nil {
			t.Fatal(err)
		}

		if emit == nil {
			break
		}
	}
}

func TestCopyIn(t *testing.T) {
	cxt := openNewFebeContext(t)
	err := cxt.SimpleQuery("CREATE TEMP TABLE foo (a int);")
	if err != nil {
		t.Fatal(err)
	}

	mustWaitForReady(t, cxt)

	sending := make(chan string)
	done := make(chan bool)

	go func() {

		err = cxt.SimpleQuery("COPY foo FROM STDIN")
		if err != nil {
			t.Fatal(err)
		}

		for {
			s, _, err := cxt.pqNext()
			if err != nil {
				t.Fatal(err)
			}

			if s == PQ_STATE_COPYIN {
				payload, ok := <-sending

				if !ok {
					cxt.copyInFinish = true
					break
				}

				cxt.copyInData = []byte(payload)
			}
		}

		close(done)
	}()

	sending <- "1\n"

	// Chunking is arbitrary in CopyIn, so test putting in
	// one record across two state machine ticks.
	sending <- "100"
	sending <- "00\n"
	close(sending)

	<-done
	mustWaitForReady(t, cxt)

	err = cxt.SimpleQuery("SELECT * FROM foo;")
	if err != nil {
		t.Fatal(err)
	}

	bufferAndCheckRows(t, cxt, `[["1"] ["10000"]]`)
}

func TestCopyOut(t *testing.T) {
	cxt := openNewFebeContext(t)
	err := cxt.SimpleQuery(
		"COPY (SELECT generate_series(1, 5)) TO STDOUT;")
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

	var resbuf bytes.Buffer

	for _, v := range copyOutRows {
		resbuf.WriteString(string(*v))
	}

	results := resbuf.String()
	expected := `1
2
3
4
5
`
	quickCompare(t, results, expected)
}
