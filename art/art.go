package art

//ar树
import (
	goArt "github.com/plar/go-adaptive-radix-tree"
)

type AdaptiveRadixTree struct {
	tree goArt.Tree
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goArt.New(),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, value interface{}) (oldVal interface{}, updated bool) {
	return art.tree.Insert(key, value)
}

func (art *AdaptiveRadixTree) Get(key []byte) interface{} {
	value, _ := art.tree.Search(key)
	return value
}

func (art *AdaptiveRadixTree) Delete(key []byte) (val interface{}, deleted bool) {
	return art.tree.Delete(key)
}

func (art *AdaptiveRadixTree) Iterator() goArt.Iterator {
	return art.tree.Iterator()
}

//PrefixScan 前缀遍历
func (art *AdaptiveRadixTree) PrefixScan(prefix []byte, count int) (keys [][]byte) {
	cb := func(node goArt.Node) bool {
		if node.Kind() != goArt.Leaf {
			return true
		}
		if count <= 0 {
			return false
		}
		keys = append(keys, node.Key())
		count--
		return true
	}

	if len(prefix) == 0 {
		art.tree.ForEach(cb)
	} else {
		art.tree.ForEachPrefix(prefix, cb)
	}
	return
}

func (art *AdaptiveRadixTree) Size() int {
	return art.tree.Size()
}
