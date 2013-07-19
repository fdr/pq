// +build go1.1

package pq

import (
	"github.com/lib/pq"
	"testing"
)

func TestCustomEncode(t *testing.T) {
	pq.RegisterCodec("CUSTOMCODEC", customCodec())
	db := openTestConnDsn(t, "codec=CUSTOMCODEC")
	defer db.Close()

	q := `
		SELECT 1 WHERE $1 = '{1,2,3}'::int[]
	`
	v1 := []int{1, 2, 3}
	r, err := db.Query(q, v1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if !r.Next() {
		if r.Err() != nil {
			t.Fatal(r.Err())
		}
		t.Fatal("expected row")
	}

	var got1 int

	err = r.Scan(&got1)
	if err != nil {
		t.Fatal(err)
	}

	if got1 != 1 {
		t.Errorf(`expected got1 == 1 got: %d`, got1)
	}
}
