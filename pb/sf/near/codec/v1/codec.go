package pbcodec

import (
	"encoding/hex"
	"time"
)

func (x *Block) ID() string {
	return x.Header.Hash.AsString()
}

func (x *Block) Number() uint64 {
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