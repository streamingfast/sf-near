package pbnear

import (
	"encoding/hex"
	"time"

	"github.com/mr-tron/base58"
)

func (x *Block) ID() string {
	return x.Header.Hash.AsString()
}

func (x *Block) Num() uint64 {
	return x.Header.Height
}

func (x *Block) LIBNum() uint64 {
	return x.Header.LastFinalBlockHeight
}

func (x *Block) PreviousID() string {
	return x.Header.PrevHash.AsString()
}

func (x *Block) Time() time.Time {
	return time.Unix(0, int64(x.Header.TimestampNanosec)).UTC()
}

func (x *CryptoHash) AsString() string {
	return hex.EncodeToString(x.Bytes)
}

func (x *CryptoHash) AsBase58String() string {
	return base58.Encode(x.Bytes)
}

// firecore.Block implementation (mostly forwarding to existing methods)

func (b *Block) GetFirehoseBlockID() string {
	return b.ID()
}

func (b *Block) GetFirehoseBlockNumber() uint64 {
	return b.Header.Height
}

func (b *Block) GetFirehoseBlockParentID() string {
	return b.PreviousID()
}

func (b *Block) GetFirehoseBlockTime() time.Time {
	return b.Time()
}

func (b *Block) GetFirehoseBlockLIBNum() uint64 {
	return b.Header.LastFinalBlockHeight
}

func (b *Block) GetFirehoseBlockParentNumber() uint64 {
	return b.Header.PrevHeight
}

func (b *Block) GetFirehoseBlockVersion() int32 {
	return 1
}
