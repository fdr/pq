package pq

import (
	"database/sql/driver"
	"fmt"
)

type row []driver.Value

type rowDescription struct {
	cols    []string
	nparams int
	ooid    []int
}

const (
	PQ_STATE_IDLE = iota
	PQ_STATE_BUSY
)

type febeState byte

type febeContext struct {
	state febeState

	cn *conn

	// Emitted by RowDescription
	desc rowDescription

	// Emitted by DataRow
	row row

	// Emitted by ErrorResponse
	err *PGError
}

type simpleQueryReq *writeBuf

func (cxt *febeContext) pqNext() (
	s febeState, emit interface{}, err error) {

	defer errRecover(&err)

	switch cxt.state {
	case PQ_STATE_BUSY:
		return cxt.pqBusyTrans()
	}

	panic("not reached")
}

func (cxt *febeContext) pqBusyTrans() (
	_ febeState, emit interface{}, err error) {

	defer errRecover(&err)

Again:
	t, r := cxt.cn.recv1()

	switch t {
	case msgCommandCompleteC:
		emit = parseComplete(r.string())
	case msgCopyInResponseG:
		panic("unimplemented: CopyInResponse")
	case msgCopyOutResponseH:
		// XXX: doing nothing...no special handling for binary
		// as-is.  I think that's for human readable output.
		//
		// flags := r.byte()
		// nCol := r.int16()

		// for i := 0; i < nCol; i += 1 {
		// 	colFlags := r.int16()
		// }
	case msgRowDescriptionT:
		// Store the RowDescription and process another
		// message instead of returning immediately, as there
		// doesn't seem to be the reason why a caller would
		// ever want to be informed of a RowDescription in and
		// of itself.  This is only to enable the yielding of
		// subsequent DataRow messages.
		n := r.int16()

		cxt.desc.cols = make([]string, n)
		cxt.desc.ooid = make([]int, n)
		for i := range cxt.desc.cols {
			cxt.desc.cols[i] = r.string()
			r.next(6)
			cxt.desc.ooid[i] = r.int32()
			r.next(8)
		}

		cxt.row = make([]driver.Value, n)
		goto Again
	case msgDataRowD:
		n := r.int16()
		for i := 0; i < len(cxt.row) && i < n; i++ {
			l := r.int32()
			if l == -1 {
				continue
			}
			cxt.row[i] = decode(r.next(l), cxt.desc.ooid[i])
		}
		emit = cxt.row
	case msgEmptyQueryResponseI:
		goto Again
	case msgErrorResponseE:
		emit = parseError(r)
	case msgReadyForQueryZ:
		fmt.Println("Got ready for query")
		cxt.state = PQ_STATE_IDLE
		emit = nil
	case msgNoticeResponseN:
		panic("unimplemented: NoticeResponse")
	}

	return cxt.state, emit, err
}

func newFebeContext(cn *conn) (_ *febeContext) {
	return &febeContext{cn: cn, state: PQ_STATE_IDLE}
}

func (cxt *febeContext) SimpleQuery(cmd string) (err error) {
	defer errRecover(&err)

	b := newWriteBuf(msgQueryQ)
	b.string(cmd)
	cxt.cn.send(b)
	cxt.state = PQ_STATE_BUSY

	return err
}
