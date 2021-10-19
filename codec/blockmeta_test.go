package codec

import (
	"container/heap"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBlockHeap_Push_Get(t *testing.T) {
	blockMetas := []*blockMeta{
		{
			id:        "id.1",
			number:    1,
			blockTime: time.Now(),
		},
		{
			id:        "id.2",
			number:    2,
			blockTime: time.Now(),
		},
		{
			id:        "id.3",
			number:    3,
			blockTime: time.Now(),
		},
	}

	h := newBlockMetaHeap(nil)
	for _, bm := range blockMetas {
		h.Push(bm)
	}

	//GET
	bm := h.get("id.1")
	require.Equal(t, bm.id, "id.1")

	bm = h.get("id.2")
	require.Equal(t, bm.id, "id.2")

	bm = h.get("id.3")
	require.Equal(t, bm.id, "id.3")

	//POP
	bm = heap.Pop(h).(*blockMeta)
	require.Equal(t, bm.id, "id.1")

	bm = heap.Pop(h).(*blockMeta)
	require.Equal(t, bm.id, "id.2")

	bm = heap.Pop(h).(*blockMeta)
	require.Equal(t, bm.id, "id.3")
}

func TestBlockHeap_Purge(t *testing.T) {
	blockMetas := []*blockMeta{
		{
			id:        "id.1",
			number:    1,
			blockTime: time.Now(),
		},
		{
			id:        "id.2",
			number:    2,
			blockTime: time.Now(),
		},
		{
			id:        "id.3",
			number:    3,
			blockTime: time.Now(),
		},
		{
			id:        "id.4",
			number:    4,
			blockTime: time.Now(),
		},
		{
			id:        "id.5",
			number:    5,
			blockTime: time.Now(),
		},
	}

	h := newBlockMetaHeap(nil)
	for _, bm := range blockMetas {
		h.Push(bm)
	}

	h.purge("id.1")
	bm := heap.Pop(h).(*blockMeta)
	require.Equal(t, "id.2", bm.id)

	h.purge("id.4")
	bm = heap.Pop(h).(*blockMeta)
	require.Equal(t, "id.5", bm.id)

	require.Equal(t, 0, h.Len())

	shouldAlsoBeNil := heap.Pop(h)
	require.Nil(t, shouldAlsoBeNil)

}

func TestBlockHeap_BlockGetter(t *testing.T) {

	getterCallCount := 0
	getter := blockMetaGetterFunc(func(id string) (*blockMeta, error) {
		getterCallCount += 1
		return &blockMeta{
			id:        "id.1",
			number:    1,
			blockTime: time.Now(),
		}, nil
	})

	h := newBlockMetaHeap(getter)

	h.get("id.1")

	require.Equal(t, 1, getterCallCount)

	bm := heap.Pop(h).(*blockMeta)
	require.Equal(t, "id.1", bm.id)

}