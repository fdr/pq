package pq

import (
	"database/sql/driver"
)

type row []driver.Value

type rowDescription struct {
	cols    []string
	nparams int
	ooid    []int
}

type pqBusyState struct {
	cn *conn

	// Emitted by RowDescription
	desc rowDescription

	// Emitted by DataRow
	row row

	// Emitted by ErrorResponse
	err *PGError
}

func (s *pqBusyState) pgBusyNext() (emit interface{}, err error) {
	defer errRecover(&err)

Again:
	t, r := s.cn.recv1()

	switch t {
	case CommandComplete:
		emit = parseComplete(r.string())
	case CopyInResponse:
		panic("unimplemented: CopyInResponse")
	case CopyOutResponse:
		panic("unimplemented: CopyOutResponse")
	case RowDescription:
		n := r.int16()

		s.desc.cols = make([]string, n)
		s.desc.ooid = make([]int, n)
		for i := range s.desc.cols {
			s.desc.cols[i] = r.string()
			r.next(6)
			s.desc.ooid[i] = r.int32()
			r.next(8)
		}

		s.row = make([]driver.Value, n)
		goto Again
	case DataRow:
		n := r.int16()
		for i := 0; i < len(s.row) && i < n; i++ {
			l := r.int32()
			if l == -1 {
				continue
			}
			s.row[i] = decode(r.next(l), s.desc.ooid[i])
		}
		emit = s.row
	case EmptyQueryResponse:
	case ErrorResponse:
		s.err = parseError(r)
	case ReadyForQuery:
		emit = nil
	case NoticeResponse:
		panic("unimplemented: NoticeResponse")
	}

	return emit, err
}



func (cn *conn) SimpleQuery(cmd string) (it *pqBusyState, err error) {
	defer errRecover(&err)

	b := newWriteBuf(Query)
	b.string(cmd)
	cn.send(b)

	return &pqBusyState{cn: cn}, err
}
