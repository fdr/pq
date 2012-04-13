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

type notification struct {
	pid     int
	channel string
	payload string
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

	// emitted by RowDescription
	desc rowDescription

	// emitted by DataRow
	row row

	// emitted by NotificationResponse
	notification notification

	// set by caller to send bulk CopyData.
	copyInData []byte

	// set by a caller to end a copy-in.
	//
	// NB: copyInData is NOT sent when set.
	copyInFinish bool
}

type simpleQueryReq *writeBuf

func (cxt *febeContext) pqNext() (s febeState, emit interface{}, err error) {
	for {
		var t pqMsgType
		var r *readBuf

		// In any state where the client is expected to
		// receive input, handle the asynchronous operations
		// that the server is allowed to emit at any time.
		if cxt.state != PQ_STATE_COPYIN {
			t, r = cxt.cn.recv1()

			switch t {
			case msgNotificationResponseA:
				return cxt.state, cxt.notification, err

			// Unimplemented, but no cause for panic
			case msgNoticeResponseN:
			case msgParameterStatusS:
			}
		}

		switch cxt.state {
		case PQ_STATE_BUSY:
			cxt.state, emit, err = cxt.pqBusyTrans(t, r)
		case PQ_STATE_COPYOUT:
			cxt.state, emit, err = cxt.pqCopyOutTrans(t, r)
		case PQ_STATE_COPYIN:
			cxt.state, emit, err = cxt.pqCopyInTrans()
		}

		// Break the loop if there's something to tell the
		// caller, or it is expected that the client needs to
		// send data.
		if err != nil || emit != nil ||
			cxt.state == PQ_STATE_IDLE ||
			cxt.state == PQ_STATE_COPYIN {
			break
		}
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

func (cxt *febeContext) pqCopyOutTrans(t pqMsgType, r *readBuf) (
	_ febeState, emit interface{}, err error) {

	defer errRecover(&err)

	switch t {
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

func (cxt *febeContext) pqBusyTrans(t pqMsgType, r *readBuf) (
	s febeState, emit interface{}, err error) {

	defer errRecover(&err)
	s = cxt.state

	switch t {
	case msgCommandCompleteC:
		emit = parseComplete(r.string())
	case msgCopyInResponseG:
		s = PQ_STATE_COPYIN
	case msgCopyOutResponseH:
		s = PQ_STATE_COPYOUT
	case msgRowDescriptionT:
		// Store the RowDescription and process another
		// message by emitting nil, as there doesn't seem to
		// be the reason why a caller would ever want to be
		// informed of a RowDescription in and of itself.
		// This is only to enable the yielding of subsequent
		// DataRow messages.
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
	case msgParseComplete1:
	case msgBindComplete2:
	case msgCloseComplete3:
	case msgBackendKeyDataK:
	case msgNoDatan:
	case msgParameterDescriptiont:
	case msgCopyBothResponseW:
		panic("unimplemented: CopyBothResponse")
	}

	return s, emit, err
}

func (cxt *febeContext) handleNotification(r *readBuf) {
	n := &cxt.notification
	n.pid = r.int32()
	n.channel = r.string()
	n.payload = r.string()
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
