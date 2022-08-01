package zset

import (
	"math"
	"math/rand"
	"sdb/bitcask"
	"sdb/utils"
)

//zset太复杂，没想好怎么写文件放磁盘，就当学习zset实现了

const (
	maxLevel    = 32
	probability = 0.25
)

type EncodeKey func(key, subKey []byte) []byte

type (
	//SortedSet 一个key对应一个有序集合
	SortedSet struct {
		record map[string]*SortedSetNode
	}

	// SortedSetNode 有序集合的数据结构，与redis一样，采用一个map加一个跳表的方式
	SortedSetNode struct {
		dict map[string]*sklNode //字典，为了O(1)查询
		skl  *skipList           //跳表，为了有序、插入、删除
	}

	sklLevel struct {
		forward *sklNode //前进指针

		//跨度，用于记录两个节点之间的距离。指向NULL的所有前进指针的跨度都为0，因为它们没有连向任何节点。
		//跨度实际上是用来计算排位（rank）的：在查找某个节点的过程中，将沿途访问过的所有层的跨度累计起来，得到的结果就是目标节点在跳跃表中的排位。
		span uint64
	}

	sklNode struct {
		//在同一个跳跃表中，各个节点保存的成员对象必须是唯一的，但是多个节点保存的分值却可以是相同的
		//分值相同的节点将按照成员对象在字典序中的大小来进行排序，成员对象较小的节点会排在前面（靠近表头的方向）
		//而成员对象较大的节点则会排在后面（靠近表尾的方向）。
		member string  //成员对象，为了简化直接用string。redis里是一个指针，指向字符串对象
		score  float64 //分值，在跳跃表中，节点按各自所保存的分值从小到大排列。

		//后退（backward）指针：它指向位于当前节点的前一个节点。
		// 后退指针在程序从表尾向表头遍历时使用。每次只能后退至前一个节点
		backward *sklNode

		level []*sklLevel //层
	}

	skipList struct { //持有跳表节点
		head   *sklNode //指向表头节点
		tail   *sklNode //指向表尾节点
		length int64    //记录跳跃表的长度，即跳跃表目前包含节点的数量（表头节点不计算在内）
		level  int16    //记录目前跳跃表内，层数最大的那个节点的层数（表头节点的层数不计算在内）
	}
)

// New create a new sorted set.
func New() *SortedSet {
	return &SortedSet{
		make(map[string]*SortedSetNode),
	}
}

func (z *SortedSet) IterateAndSend(chn chan *bitcask.LogRecord, encode EncodeKey) {
	for key, ss := range z.record {
		zsetKey := []byte(key)
		if ss.skl.head == nil {
			return
		}
		for e := ss.skl.head.level[0].forward; e != nil; e = e.level[0].forward {
			scoreBuf := []byte(utils.Float64ToStr(e.score))
			encKey := encode(zsetKey, scoreBuf)
			chn <- &bitcask.LogRecord{Key: encKey, Value: []byte(e.member)}
		}
	}
	return
}

// ZAdd Adds the specified member with the specified score to the sorted set stored at key.
func (z *SortedSet) ZAdd(key string, score float64, member string) {
	if !z.exist(key) { //不存在，创建key的新zSet
		node := &SortedSetNode{
			dict: make(map[string]*sklNode),
			skl:  newSkipList(),
		}
		z.record[key] = node
	}

	item := z.record[key]
	v, exist := item.dict[member]

	var node *sklNode
	if exist { //存在的话需要看下跳表中是否有这个member，因为member要求不重复
		if score != v.score { //一样的分值，一样的member直接跳过，不一样的更新分值
			//因为分值更改后可能会影响排序
			item.skl.sklDelete(v.score, member)      //先删
			node = item.skl.sklInsert(score, member) //后插
		}
	} else { //新建的zSet肯定没有，大胆插入
		node = item.skl.sklInsert(score, member)
	}

	if node != nil { //如果没跳过，更新下字典映射
		item.dict[member] = node
	}
}

func sklNewNode(level int16, score float64, member string) *sklNode {
	node := &sklNode{
		score:  score,
		member: member,
		level:  make([]*sklLevel, level),
	}

	for i := range node.level {
		node.level[i] = new(sklLevel)
	}

	return node
}

func newSkipList() *skipList {
	return &skipList{ //头节点
		level: 1,
		head:  sklNewNode(maxLevel, 0, ""), //头节点层数是最大层数，一开始都指向nik，是0
	}
}

func randomLevel() int16 { //随机层数
	var level int16 = 1
	for float32(rand.Int31()&0xFFFF) < (probability * 0xFFFF) {
		level++
	}

	if level < maxLevel {
		return level
	}

	return maxLevel
}

//sklInsert 跳表插入
func (skl *skipList) sklInsert(score float64, member string) *sklNode {
	updates := make([]*sklNode, maxLevel) //updates[i]表示i层指向新节点的节点
	rank := make([]uint64, maxLevel)      //排位，rank[i]记录的是转正点，所谓转正点就是遍历跳表到一个节点后要向下层遍历
	//rank[i]表示：假设对于新节点，i层指向它的节点为f，则头节点到f的span为rank[i]
	//ran[0]+1一定能表示头节点到新节点的span即新节点的rank

	p := skl.head
	for i := skl.level - 1; i >= 0; i-- { //从最高层向最低层遍历
		if i == skl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1] //初始化为上一轮的rank
		}

		if p.level[i] != nil {
			nextNode := p.level[i].forward
			for nextNode != nil && //在该层进行循环遍历
				(nextNode.score < score || //新的分值比当前遍历节点的后后置节点分值大，后移
					(nextNode.score == score && nextNode.member < member)) { //分值一样按字典序
				//跨度累加
				rank[i] += p.level[i].span //加上这个的跨度
				p = nextNode
			}
			//找到了要插到后面的节点
		}
		updates[i] = p //记录下谁指向新节点
	}

	level := randomLevel() //随机产生层数
	if level > skl.level { //新随机的层数比现在最大层数还大
		for i := skl.level; i < level; i++ { //对于第一个循环没有遍历到的层，需要新增
			rank[i] = 0                                   //0表示i层指向新节点就是头节点，头节点到头节点的span是0，很合理
			updates[i] = skl.head                         //记录i层指向新节点的节点就是头节点
			updates[i].level[i].span = uint64(skl.length) //先把跨度初始化为最大跨度
		}
		skl.level = level //更新最大层数
	}

	p = sklNewNode(level, score, member) //new一个新节点
	for i := int16(0); i < level; i++ {  //遍历新节点的层数
		frontNode := updates[i].level[i]
		p.level[i].forward = frontNode.forward //插入链表的操作，新节点指向原节点的后一个节点
		frontNode.forward = p                  //原节点指向新节点

		p.level[i].span = (frontNode.span + 1) - (rank[0] - rank[i] + 1) //新节点的span就是前向节点到后向节点的span+1（因为插进去多了1距离）减去 前向节点到新节点的距离
		frontNode.span = rank[0] + 1 - rank[i]                           //新节点的前向节点指向新节点的span是头节点到新节点的span减去头节点到前向节点的span
	}

	for i := level; i < skl.level; i++ { //如果原来的层数比新节点的层数大，把剩下的层数的当前节点前面的节点的span都+1
		updates[i].level[i].span++
	}

	if updates[0] == skl.head { //如果紧挨当前节点的上一个节点是头节点，后退指针指向null
		p.backward = nil
	} else { //否则指向紧挨的上一个节点
		p.backward = updates[0]
	}

	if p.level[0].forward != nil { //紧挨当前节点的后一个节点指向当前节点
		p.level[0].forward.backward = p
	} else { //否则说明新节点是尾节点，tail指向新节点
		skl.tail = p
	}

	skl.length++ //长度+1
	return p
}

//sklDeleteNode 跳表删除节点
func (skl *skipList) sklDeleteNode(p *sklNode, updates []*sklNode) {
	for i := int16(0); i < skl.level; i++ {
		if updates[i].level[i].forward == p { //对于后置节点是要删除节点的节点
			updates[i].level[i].span += p.level[i].span - 1  //span加上删除的节点的span减一（因为删除了一个节点）
			updates[i].level[i].forward = p.level[i].forward //指针指向删除节点的后置节点
		} else { //其他节点直接span减一
			updates[i].level[i].span--
		}
	}

	//回退指针改变
	if p.level[0].forward != nil { //不是尾节点
		p.level[0].forward.backward = p.backward //前置节点指向后置节点
	} else { //是尾节点
		skl.tail = p.backward //尾节点指针指向前置节点
	}

	for skl.level > 1 && skl.head.level[skl.level-1].forward == nil { //删去头节点的forward指针指向nil的层数，说明这层没有成员节点
		skl.level--
	}

	//长度直接减一
	skl.length--
}

func (skl *skipList) sklDelete(score float64, member string) {
	update := make([]*sklNode, maxLevel)
	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		nextNode := p.level[i].forward
		for nextNode != nil &&
			(nextNode.score < score ||
				(nextNode.score == score && nextNode.member < member)) {
			p = nextNode
		}
		update[i] = p //转折点
	}

	p = p.level[0].forward                                  //nextNode有可能是要找的，要么大于要么等于                                  //
	if p != nil && score == p.score && p.member == member { //比较确实是要删除的
		skl.sklDeleteNode(p, update) //删除节点
		return
	}
	//不是的话不做任何操作
}

// ZScore returns the score of member in the sorted set at key.
func (z *SortedSet) ZScore(key string, member string) (ok bool, score float64) {
	if !z.exist(key) {
		return
	}

	node, exist := z.record[key].dict[member]
	if !exist {
		return
	}

	return true, node.score
}

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at key.
func (z *SortedSet) ZCard(key string) int {
	if !z.exist(key) {
		return 0
	}

	return len(z.record[key].dict)
}

// ZRank returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
// The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.
func (z *SortedSet) ZRank(key, member string) int64 {
	if !z.exist(key) {
		return -1
	}

	v, exist := z.record[key].dict[member]
	if !exist {
		return -1
	}

	rank := z.record[key].skl.sklGetRank(v.score, member)
	rank--

	return rank
}

// ZRevRank returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
// The rank (or index) is 0-based, which means that the member with the highest score has rank 0.
func (z *SortedSet) ZRevRank(key, member string) int64 {
	if !z.exist(key) {
		return -1
	}

	v, exist := z.record[key].dict[member]
	if !exist {
		return -1
	}

	rank := z.record[key].skl.sklGetRank(v.score, member)

	return z.record[key].skl.length - rank
}

// ZIncrBy increments the score of member in the sorted set stored at key by increment.
// If member does not exist in the sorted set, it is added with increment as its score (as if its previous score was 0.0).
// If key does not exist, a new sorted set with the specified member as its sole member is created.
func (z *SortedSet) ZIncrBy(key string, increment float64, member string) float64 {
	if z.exist(key) {
		node, exist := z.record[key].dict[member]
		if exist {
			increment += node.score
		}
	}

	z.ZAdd(key, increment, member)
	return increment
}

// ZRange returns the specified range of elements in the sorted set stored at <key>.
func (z *SortedSet) ZRange(key string, start, stop int) []interface{} {
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), false, false)
}

// ZRangeWithScores returns the specified range of elements in the sorted set stored at <key>.
func (z *SortedSet) ZRangeWithScores(key string, start, stop int) []interface{} {
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), false, true)
}

// ZRevRange returns the specified range of elements in the sorted set stored at key.
// The elements are considered to be ordered from the highest to the lowest score.
// Descending lexicographical order is used for elements with equal score.
func (z *SortedSet) ZRevRange(key string, start, stop int) []interface{} {
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), true, false)
}

// ZRevRangeWithScores returns the specified range of elements in the sorted set stored at key.
// The elements are considered to be ordered from the highest to the lowest score.
// Descending lexicographical order is used for elements with equal score.
func (z *SortedSet) ZRevRangeWithScores(key string, start, stop int) []interface{} {
	if !z.exist(key) {
		return nil
	}

	return z.findRange(key, int64(start), int64(stop), true, true)
}

// ZRem removes the specified members from the sorted set stored at key. Non existing members are ignored.
// An error is returned when key exists and does not hold a sorted set.
func (z *SortedSet) ZRem(key, member string) bool {
	if !z.exist(key) {
		return false
	}

	v, exist := z.record[key].dict[member]
	if exist {
		z.record[key].skl.sklDelete(v.score, member)
		delete(z.record[key].dict, member)
		return true
	}

	return false
}

// ZGetByRank get the member at key by rank, the rank is ordered from lowest to highest.
// The rank of lowest is 0 and so on.
func (z *SortedSet) ZGetByRank(key string, rank int) (val []interface{}) {
	if !z.exist(key) {
		return
	}

	member, score := z.getByRank(key, int64(rank), false)
	val = append(val, member, score)
	return
}

// ZRevGetByRank get the member at key by rank, the rank is ordered from highest to lowest.
// The rank of highest is 0 and so on.
func (z *SortedSet) ZRevGetByRank(key string, rank int) (val []interface{}) {
	if !z.exist(key) {
		return
	}

	member, score := z.getByRank(key, int64(rank), true)
	val = append(val, member, score)
	return
}

// ZScoreRange returns all the elements in the sorted set at key with a score between min and max (including elements with score equal to min or max).
// The elements are considered to be ordered from low to high scores.
func (z *SortedSet) ZScoreRange(key string, min, max float64) (val []interface{}) {
	if !z.exist(key) || min > max {
		return
	}

	item := z.record[key].skl
	minScore := item.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}

	maxScore := item.tail.score
	if max > maxScore {
		max = maxScore
	}

	p := item.head
	for i := item.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && p.level[i].forward.score < min {
			p = p.level[i].forward
		}
	}

	p = p.level[0].forward
	for p != nil {
		if p.score > max {
			break
		}

		val = append(val, p.member, p.score)
		p = p.level[0].forward
	}

	return
}

// ZRevScoreRange returns all the elements in the sorted set at key with a score between max and min (including elements with score equal to max or min).
// In contrary to the default ordering of sorted sets, for this command the elements are considered to be ordered from high to low scores.
func (z *SortedSet) ZRevScoreRange(key string, max, min float64) (val []interface{}) {
	if !z.exist(key) || max < min {
		return
	}

	item := z.record[key].skl
	minScore := item.head.level[0].forward.score
	if min < minScore {
		min = minScore
	}

	maxScore := item.tail.score
	if max > maxScore {
		max = maxScore
	}

	p := item.head
	for i := item.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && p.level[i].forward.score <= max {
			p = p.level[i].forward
		}
	}

	for p != nil {
		if p.score < min {
			break
		}

		val = append(val, p.member, p.score)
		p = p.backward
	}

	return
}

// ZKeyExists check if the key exists in zset.
func (z *SortedSet) ZKeyExists(key string) bool {
	return z.exist(key)
}

// ZClear clear the key in zset.
func (z *SortedSet) ZClear(key string) {
	if z.ZKeyExists(key) {
		delete(z.record, key)
	}
}

//exist judge if the sortedSet of the key exists
func (z *SortedSet) exist(key string) bool {
	_, exist := z.record[key]
	return exist
}

func (z *SortedSet) getByRank(key string, rank int64, reverse bool) (string, float64) {

	skl := z.record[key].skl
	if rank < 0 || rank > skl.length {
		return "", math.MinInt64
	}

	if reverse {
		rank = skl.length - rank
	} else {
		rank++
	}

	n := skl.sklGetElementByRank(uint64(rank))
	if n == nil {
		return "", math.MinInt64
	}

	node := z.record[key].dict[n.member]
	if node == nil {
		return "", math.MinInt64
	}

	return node.member, node.score
}

func (z *SortedSet) findRange(key string, start, stop int64, reverse bool, withScores bool) (val []interface{}) {
	skl := z.record[key].skl
	length := skl.length

	if start < 0 {
		start += length
		if start < 0 {
			start = 0
		}
	}

	if stop < 0 {
		stop += length
	}

	if start > stop || start >= length {
		return
	}

	if stop >= length {
		stop = length - 1
	}
	span := (stop - start) + 1

	var node *sklNode
	if reverse {
		node = skl.tail
		if start > 0 {
			node = skl.sklGetElementByRank(uint64(length - start))
		}
	} else {
		node = skl.head.level[0].forward
		if start > 0 {
			node = skl.sklGetElementByRank(uint64(start + 1))
		}
	}

	for span > 0 {
		span--
		if withScores {
			val = append(val, node.member, node.score)
		} else {
			val = append(val, node.member)
		}
		if reverse {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}

	return
}

func (skl *skipList) sklGetRank(score float64, member string) int64 {
	var rank uint64 = 0
	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil &&
			(p.level[i].forward.score < score ||
				(p.level[i].forward.score == score && p.level[i].forward.member <= member)) {

			rank += p.level[i].span
			p = p.level[i].forward
		}

		if p.member == member {
			return int64(rank)
		}
	}

	return 0
}

func (skl *skipList) sklGetElementByRank(rank uint64) *sklNode {
	var traversed uint64 = 0
	p := skl.head

	for i := skl.level - 1; i >= 0; i-- {
		for p.level[i].forward != nil && (traversed+p.level[i].span) <= rank {
			traversed += p.level[i].span
			p = p.level[i].forward
		}
		if traversed == rank {
			return p
		}
	}

	return nil
}
