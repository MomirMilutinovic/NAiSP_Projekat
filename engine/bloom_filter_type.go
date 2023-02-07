package engine

import bloomfilter "github.com/MomirMilutinovic/NAiSP_Projekat/bloom-filter"

func TransformKeyToBloomFilterKey(key string) string {
	return "bloomfilter." + key
}

func (engine *DB) CreateBloomFilter(key string, m uint, k uint) (success bool) {

	if m == 0 || k == 0 {
		return false
	}

	bloom_filter := bloomfilter.CreateBloomFilter(m, k)
	serialized_bloom_filter := bloom_filter.Serialize()

	return engine.Put(TransformKeyToBloomFilterKey(key), serialized_bloom_filter)
}

func (engine *DB) IsStringPossiblyInBloomFilter(key string, value string) (possibly_present, bloom_filter_exists bool) {
	serialized_bloom_filter := engine.Get(TransformKeyToBloomFilterKey(key))

	if serialized_bloom_filter != nil {
		bloom_filter := bloomfilter.Deserialize(serialized_bloom_filter)
		return bloom_filter.Find([]byte(value)), true
	} else {
		return false, false
	}
}

func (engine *DB) AddStringToBloomFilter(key string, new_value string) bool {
	serialized_bloom_filter := engine.Get(TransformKeyToBloomFilterKey(key))

	if serialized_bloom_filter != nil {
		bloom_filter := bloomfilter.Deserialize(serialized_bloom_filter)
		bloom_filter.Add([]byte(new_value))
		return engine.Put(TransformKeyToBloomFilterKey(key), bloom_filter.Serialize())
	} else {
		return false
	}
}

func (engine *DB) AddBatchOfStringToBloomFilter(key string, values []string) (success bool) {
	serialized_bloom_filter := engine.Get(TransformKeyToBloomFilterKey(key))

	if serialized_bloom_filter != nil {
		bloom_filter := bloomfilter.Deserialize(serialized_bloom_filter)
		for _, new_value := range values {
			bloom_filter.Add([]byte(new_value))
		}
		return engine.Put(TransformKeyToBloomFilterKey(key), bloom_filter.Serialize())
	} else {
		return false
	}
}

func (engine *DB) DeleteBloomFilter(key string) (success bool) {
	return engine.Delete(TransformKeyToBloomFilterKey(key))
}
