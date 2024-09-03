package codec

import (
	"bufio"
	"container/heap"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/mr-tron/base58"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

	"github.com/streamingfast/bstream"
	firecore "github.com/streamingfast/firehose-core"
	pbnear "github.com/streamingfast/firehose-near/pb/sf/near/type/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const FirePrefixLen = len("FIRE ")

// ConsoleReader is what reads the `geth` output directly. It builds
// up some LogEntry objects. See `LogReader to read those entries .
type ConsoleReader struct {
	lines        chan string
	blockEncoder firecore.BlockEncoder
	close        func()

	ctx  *parseCtx
	done chan interface{}
}

func NewConsoleReader(lines chan string, blockEncoder firecore.BlockEncoder, rpcUrl string) (*ConsoleReader, error) {
	l := &ConsoleReader{
		lines:        lines,
		blockEncoder: blockEncoder,
		close:        func() {},
		ctx: &parseCtx{
			blockMetas: newBlockMetaHeap(NewRPCBlockMetaGetter(rpcUrl)),
		},
		done: make(chan interface{}),
	}
	return l, nil
}

// todo: WTF?
func (r *ConsoleReader) Done() <-chan interface{} {
	return r.done
}

func (r *ConsoleReader) Close() {
	r.close()
}

type parsingStats struct {
	startAt  time.Time
	blockNum uint64
	data     map[string]int
}

func newParsingStats(block uint64) *parsingStats {
	return &parsingStats{
		startAt:  time.Now(),
		blockNum: block,
		data:     map[string]int{},
	}
}

func (s *parsingStats) log() {
	zlog.Info("reader block stats",
		zap.Uint64("block_num", s.blockNum),
		zap.Duration("duration", time.Since(s.startAt)),
		zap.Reflect("stats", s.data),
	)
}

type parseCtx struct {
	blockMetas *blockMetaHeap
}

func (r *ConsoleReader) ReadBlock() (out *pbbstream.Block, err error) {
	block, err := r.next(readBlock)
	if err != nil {
		return nil, err
	}

	return r.blockEncoder.Encode(block)
}

const (
	readBlock = 1
)

func (r *ConsoleReader) next(readType int) (out *pbnear.Block, err error) {
	ctx := r.ctx

	zlog.Debug("next", zap.Int("read_type", readType))

	for line := range r.lines {
		if !strings.HasPrefix(line, "FIRE ") {
			continue
		}

		line = line[FirePrefixLen:]

		switch {
		case strings.HasPrefix(line, "BLOCK"):
			out, err = ctx.readBlock(line)
		default:
			if tracer.Enabled() {
				zlog.Debug("skipping unknown Firehose log line", zap.String("line", line))
			}

			continue
		}

		if err != nil {
			chunks := strings.SplitN(line, " ", 2)
			return nil, fmt.Errorf("%s: %s (line %q)", chunks[0], err, line)
		}

		if out != nil {
			return out, nil
		}
	}

	zlog.Info("lines channel has been closed")
	return nil, io.EOF
}

func (r *ConsoleReader) ProcessData(reader io.Reader) error {
	scanner := r.buildScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		r.lines <- line
	}

	if scanner.Err() == nil {
		close(r.lines)
		return io.EOF
	}

	return scanner.Err()
}

func (r *ConsoleReader) buildScanner(reader io.Reader) *bufio.Scanner {
	buf := make([]byte, 50*1024*1024)
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(buf, 50*1024*1024)

	return scanner
}

// Formats
// FIRE BLOCK {height} {hash} {parent_height} {parent_hash} {lib} {timestamp} {hex}
func (ctx *parseCtx) readBlock(line string) (*pbnear.Block, error) {
	chunks, err := SplitInChunks(line, 8)
	if err != nil {
		var invalidSplitErr *InvalidSplitError
		if errors.As(err, &invalidSplitErr) {
			if invalidSplitErr.actual == 4 {
				return ctx.readBlockOldVersion(line) //backward compatibility
			} else {
				return nil, fmt.Errorf("split: %s", err)
			}
		} else {
			return nil, fmt.Errorf("split: %s", err)
		}
	}

	blockNum, err := strconv.ParseUint(chunks[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid block num: %w", err)
	}

	_, err = hex.DecodeString(chunks[1])
	if err != nil {
		return nil, fmt.Errorf("invalid block hash: %w", err)
	}

	parentHeight, err := strconv.ParseUint(chunks[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid parent height: %w", err)
	}

	parentHash, err := hex.DecodeString(chunks[3])
	if err != nil {
		return nil, fmt.Errorf("invalid parent hash: %w", err)
	}

	libHash, err := hex.DecodeString(chunks[4])
	if err != nil {
		return nil, fmt.Errorf("invalid lib hash: %w", err)
	}

	// We skip block hash for now
	protoBytes, err := hex.DecodeString(chunks[6])
	if err != nil {
		return nil, fmt.Errorf("invalid block bytes: %w", err)
	}

	block := &pbnear.Block{}
	if err := proto.Unmarshal(protoBytes, block); err != nil {
		return nil, fmt.Errorf("invalid block: %w", err)
	}

	if block.Header.PrevHeight != parentHeight {
		return nil, fmt.Errorf("invalid block: prev height mismatch, got %d, expected %d", block.Header.PrevHeight, parentHeight)
	}
	if block.Header.PrevHash.AsBase58String() != base58.Encode(parentHash) {
		return nil, fmt.Errorf("invalid block: prev hash mismatch, got %s, expected %s", block.Header.PrevHash.AsBase58String(), base58.Encode(parentHash))
	}
	if block.Header.LastFinalBlock.AsBase58String() != hex.EncodeToString(libHash) {
		return nil, fmt.Errorf("invalid block: lib hash mismatch, got %s, expected %s", block.Header.LastFinalBlock.AsBase58String(), hex.EncodeToString(libHash))
	}

	newParsingStats(blockNum).log()

	//Push new block meta
	ctx.blockMetas.Push(&blockMeta{
		id:        block.Header.Hash.AsBase58String(),
		number:    block.Num(),
		blockTime: block.Time(),
	})

	//Setting previous height
	prevHeightId := block.Header.PrevHash.AsBase58String()
	if prevHeightId == "11111111111111111111111111111111" { // block id 0 (does not exist)
		block.Header.PrevHeight = bstream.GetProtocolFirstStreamableBlock
	} else {
		if block.Header.PrevHeight == 0 {
			prevHeightMeta, err := ctx.blockMetas.get(prevHeightId)
			if err != nil {
				return nil, fmt.Errorf("getting prev height meta: %w", err)
			}
			block.Header.PrevHeight = prevHeightMeta.number
		}
	}

	//Setting LIB num
	lastFinalBlockId := block.Header.LastFinalBlock.AsBase58String()
	if lastFinalBlockId == "11111111111111111111111111111111" { // block id 0 (does not exist)
		block.Header.LastFinalBlockHeight = bstream.GetProtocolFirstStreamableBlock
	} else {
		libBlockMeta, err := ctx.blockMetas.get(lastFinalBlockId)
		if err != nil {
			return nil, fmt.Errorf("getting lib block meta: %w", err)
		}
		block.Header.LastFinalBlockHeight = libBlockMeta.number
	}

	//Purging
	for {
		if ctx.blockMetas.Len() <= 2000 {
			break
		}
		heap.Pop(ctx.blockMetas)
	}

	return block, nil
}

func (ctx *parseCtx) readBlockOldVersion(line string) (*pbnear.Block, error) {
	chunks, err := SplitInChunks(line, 4)
	if err != nil {
		return nil, fmt.Errorf("split: %s", err)
	}

	blockNum, err := strconv.ParseUint(chunks[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid block num: %w", err)
	}

	// We skip block hash for now
	protoBytes, err := hex.DecodeString(chunks[2])
	if err != nil {
		return nil, fmt.Errorf("invalid block bytes: %w", err)
	}

	block := &pbnear.Block{}
	if err := proto.Unmarshal(protoBytes, block); err != nil {
		return nil, fmt.Errorf("invalid block: %w", err)
	}

	newParsingStats(blockNum).log()

	//Push new block meta
	ctx.blockMetas.Push(&blockMeta{
		id:        block.Header.Hash.AsBase58String(),
		number:    block.Num(),
		blockTime: block.Time(),
	})

	//Setting previous height
	prevHeightId := block.Header.PrevHash.AsBase58String()
	if prevHeightId == "11111111111111111111111111111111" { // block id 0 (does not exist)
		block.Header.PrevHeight = bstream.GetProtocolFirstStreamableBlock
	} else {
		prevHeightMeta, err := ctx.blockMetas.get(prevHeightId)
		if err != nil {
			return nil, fmt.Errorf("getting prev height meta: %w", err)
		}
		block.Header.PrevHeight = prevHeightMeta.number
	}

	//Setting LIB num
	lastFinalBlockId := block.Header.LastFinalBlock.AsBase58String()
	if lastFinalBlockId == "11111111111111111111111111111111" { // block id 0 (does not exist)
		block.Header.LastFinalBlockHeight = bstream.GetProtocolFirstStreamableBlock
	} else {
		libBlockMeta, err := ctx.blockMetas.get(lastFinalBlockId)
		if err != nil {
			return nil, fmt.Errorf("getting lib block meta: %w", err)
		}
		block.Header.LastFinalBlockHeight = libBlockMeta.number
	}

	//Purging
	for {
		if ctx.blockMetas.Len() <= 2000 {
			break
		}
		heap.Pop(ctx.blockMetas)
	}

	return block, nil
}

// splitInChunks split the line in `count` chunks and returns the slice `chunks[1:count]` (so exclusive end), but verifies
// that there are only exactly `count` chunks, and nothing more.

func SplitInChunks(line string, count int) ([]string, error) {
	chunks := strings.SplitN(line, " ", -1)
	if len(chunks) != count {
		return nil, &InvalidSplitError{expected: count, actual: len(chunks)}
	}

	return chunks[1:count], nil
}

type InvalidSplitError struct {
	expected int
	actual   int
}

func (i *InvalidSplitError) Error() string {
	return fmt.Sprintf("invalid split, expected %d chunks, got %d", i.expected, i.actual)
}
