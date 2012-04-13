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

const (
	PQ_STATE_IDLE = iota
	PQ_STATE_BUSY
	PQ_STATE_COPYOUT
	PQ_STATE_COPYIN
)

type febeState byte

type febeContext struct {
	state febeState

	cn *conn

	// Emitted by RowDescription
	desc rowDescription

	// Emitted by DataRow
	row row

	// Set by a caller to send bulk CopyData to the server
	copyInData []byte

	// Ends a copy-in.  NB: copyInData is NOT sent when set
	copyInFinish bool
}

type simpleQueryReq *writeBuf

func (cxt *febeContext) pqNext() (s febeState, emit interface{}, err error) {
	for {
		switch cxt.state {
		case PQ_STATE_BUSY:
			cxt.state, emit, err = cxt.pqBusyTrans()
		case PQ_STATE_COPYOUT:
			cxt.state, emit, err = cxt.pqCopyOutTrans()
		case PQ_STATE_COPYIN:
			cxt.state, emit, err = cxt.pqCopyInTrans()
		}

		// Break the loop if there's something to tell the
		// caller, or the connection is idle and ready to be
		// used again.
		if err != nil || emit != nil || cxt.state == PQ_STATE_IDLE ||
			cxt.state == PQ_STATE_COPYIN {
			break
		}

		// There is nothing useful to report to the caller, so
		// run the state machine again until there is.
	}

	return cxt.state, emit, err
}

type copyFailed string
type copyOutData *readBuf

func (cxt *febeContext) pqCopyInTrans() (
	_ febeState, emit interface{}, err error) {

	defer errRecover(&err)

	if cxt.state != PQ_STATE_COPYIN {
		panic("pq expects copy-in state")
	}

	if cxt.copyInFinish {
		cxt.cn.send(newWriteBuf(msgCopyDonec))
		cxt.state = PQ_STATE_BUSY
	} else {
		w := newWriteBuf(msgCopyDatad)
		w.bytes(cxt.copyInData)
		cxt.cn.send(w)
	}

	return cxt.state, emit, err
}

func (cxt *febeContext) pqCopyOutTrans() (
	_ febeState, emit interface{}, err error) {

	defer errRecover(&err)

	switch t, r := cxt.cn.recv1(); t {
	case msgCopyDatad:
		// The whole readbuf is the emission.
		return PQ_STATE_COPYOUT, copyOutData(r), err
	case msgCopyDonec:
		return PQ_STATE_BUSY, nil, err
	case msgCopyFailf:
		return PQ_STATE_BUSY, copyFailed(r.string()), err
	default:
		errorf("unrecognized message %q", t)
	}

	panic("not reached")
}

func (cxt *febeContext) pqBusyTrans() (
	s febeState, emit interface{}, err error) {

	defer errRecover(&err)
	s = cxt.state

	t, r := cxt.cn.recv1()

	switch t {
	case msgCommandCompleteC:
		emit = parseComplete(r.string())
	case msgCopyInResponseG:
		s = PQ_STATE_COPYIN
	case msgCopyOutResponseH:
		s = PQ_STATE_COPYOUT
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

		if emit != nil {
			panic("emit must be nil in this context")
		}
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
		if emit != nil {
			panic("emit must be nil in this context")
		}
	case msgErrorResponseE:
		emit = parseError(r)
	case msgReadyForQueryZ:
		s = PQ_STATE_IDLE

		if emit != nil {
			panic("emit must be nil in this context")
		}
	case msgNoticeResponseN:
		panic("unimplemented: NoticeResponse")
	}

	return s, emit, err
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
