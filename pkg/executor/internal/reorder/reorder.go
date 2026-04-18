// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package reorder provides a generic sequence-preserving reorder buffer for
// parallel executors that fan out work to concurrent workers and need to
// reassemble results in the original input order.
//
// The core pattern: a producer assigns monotonically increasing sequence
// numbers to work items. Multiple workers process items concurrently and
// send results (tagged with sequence numbers) to an input channel. The
// reorder worker drains results in strict sequence order, batching rows
// into output chunks for the consumer.
//
// This package is used by ParallelNestedLoopApplyExec and parallel
// StreamAggExec to share the reorder logic rather than duplicating it.
package reorder

import (
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// SeqResult is a sequence-tagged result from a parallel worker.
// T is the domain-specific payload (e.g., []*chunk.Chunk for apply,
// *chunk.Chunk for stream agg).
type SeqResult[T any] struct {
	Val T
	Err error
	Seq uint64
}

// AppendRow appends a single row to the output stream. It handles flushing
// full output chunks and acquiring new ones from the free pool. Returns true
// if the caller should exit (e.g., exit channel closed during flush).
type AppendRow func(row chunk.Row) bool

// RowEmitter extracts rows from a worker result and feeds them to the
// output stream via the provided AppendRow function. It is called by Run
// for each result in sequence order.
//
// The emitter should call appendRow for each row in the result. If appendRow
// returns true, the emitter must return true immediately (exit signalled).
// The emitter's own return value of true also signals exit.
type RowEmitter[T any] func(appendRow AppendRow, val T) bool

// SendChunk delivers a full or partial output chunk to the consumer.
// Returns true if the caller should exit (e.g., exit channel closed).
type SendChunk func(chk *chunk.Chunk, err error) bool

// Run executes the reorder worker loop. It collects out-of-order results
// from inputCh, buffers them in a pending map, and emits rows in strict
// sequence order via the emitRows callback. Output is batched into chunks
// from freeChkCh and delivered via sendResult.
//
// Backpressure: the caller should provide a paceCh where the producer
// acquires a token before dispatching each work item. Run releases one
// token (reads from paceCh) each time it advances to the next sequence.
//
// Shutdown: closing the exit channel causes Run to return promptly.
// Closing inputCh signals that all workers are done; Run drains any
// remaining buffered results and sends an EOF (sendResult with nil chunk).
func Run[T any](
	inputCh <-chan SeqResult[T],
	emitRows RowEmitter[T],
	sendResult SendChunk,
	freeChkCh chan *chunk.Chunk,
	paceCh <-chan struct{},
	exit <-chan struct{},
) {
	pending := make(map[uint64]SeqResult[T])
	nextSeq := uint64(0)

	// Get the first output chunk from the free pool.
	var outputChk *chunk.Chunk
	select {
	case outputChk = <-freeChkCh:
	case <-exit:
		return
	}

	exited := false

	flushOutput := func() {
		if sendResult(outputChk, nil) {
			exited = true
			return
		}
		select {
		case newOutput := <-freeChkCh:
			// Reset is required because the consumer may reuse the
			// same req chunk across Next() calls (e.g. writeChunks).
			// After SwapColumns the recycled chunk can carry leftover
			// column data; Reset clears it before we append new rows.
			newOutput.Reset()
			outputChk = newOutput
		case <-exit:
			exited = true
		}
	}

	// appendRow appends a single row to the current output chunk, flushing
	// when full. This is passed to emitRows so multi-row results can flush
	// mid-stream without the emitter needing to know about chunk management.
	appendRow := func(row chunk.Row) bool {
		outputChk.AppendRow(row)
		if outputChk.IsFull() {
			flushOutput()
		}
		return exited
	}

	for {
		select {
		case r, ok := <-inputCh:
			if !ok {
				// Channel closed – all workers done. Flush remaining rows.
				if outputChk.NumRows() > 0 {
					sendResult(outputChk, nil)
				}
				sendResult(nil, nil) // signal EOF
				return
			}
			if r.Err != nil {
				sendResult(nil, r.Err)
				return
			}
			pending[r.Seq] = r

			// Drain in-order results and opportunistically batch more
			// arrivals before flushing, so non-LIMIT queries get full
			// chunks while LIMIT queries still receive rows promptly
			// when the pipeline is idle.
			for {
				// Drain as many consecutive results as possible.
				for {
					pr, exists := pending[nextSeq]
					if !exists {
						break
					}
					delete(pending, nextSeq)
					nextSeq++
					<-paceCh

					if emitRows(appendRow, pr.Val) {
						return
					}
					if exited {
						return
					}
				}

				if outputChk.NumRows() == 0 {
					break // nothing to flush
				}
				// Check if more results are immediately available;
				// if so, buffer them and re-drain before flushing.
				select {
				case next, ok2 := <-inputCh:
					if !ok2 {
						sendResult(outputChk, nil)
						sendResult(nil, nil)
						return
					}
					if next.Err != nil {
						sendResult(nil, next.Err)
						return
					}
					pending[next.Seq] = next
					continue // re-drain with the new result
				default:
					// No more results ready — flush now.
				}
				flushOutput()
				if exited {
					return
				}
				break
			}

		case <-exit:
			return
		}
	}
}
