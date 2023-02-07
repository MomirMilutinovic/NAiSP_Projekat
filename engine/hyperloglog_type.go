package engine

import "github.com/MomirMilutinovic/NAiSP_Projekat/hyperloglog"

func TransformKeyToHLLKey(key string) string {
	return "hll." + key
}

func (engine *DB) CreateHyperLogLog(hll_key string, precision uint8) (success bool) {

	if precision > hyperloglog.HLL_MAX_PRECISION || precision < hyperloglog.HLL_MIN_PRECISION {
		return false
	}

	hll_key_in_db := TransformKeyToHLLKey(hll_key)

	new_hll := hyperloglog.CreateHLL(precision)

	serialized_hll := new_hll.Serialize()

	return engine.Put(hll_key_in_db, serialized_hll)
}

func (engine *DB) AddValueToHyperLogLog(hll_key string, value string) (success bool) {
	hll_key_in_db := TransformKeyToHLLKey(hll_key)

	serialized_hll := engine.Get(hll_key_in_db)

	hll := hyperloglog.DeserializeHLL(serialized_hll)

	if hll == nil {
		return false
	}

	hll.Add([]byte(value))

	serialized_hll = hll.Serialize()

	return engine.Put(hll_key_in_db, serialized_hll)
}

func (engine *DB) AddBatchOfValuesToHyperLogLog(hll_key string, values []string) (success bool) {
	hll_key_in_db := TransformKeyToHLLKey(hll_key)

	serialized_hll := engine.Get(hll_key_in_db)

	hll := hyperloglog.DeserializeHLL(serialized_hll)

	if hll == nil {
		return false
	}

	for _, value := range values {
		hll.Add([]byte(value))
	}

	serialized_hll = hll.Serialize()

	return engine.Put(hll_key_in_db, serialized_hll)
}

func (engine *DB) EstimateHyperLogLog(hll_key string) (estimate float64, success bool) {
	hll_key_in_db := TransformKeyToHLLKey(hll_key)

	serialized_hll := engine.Get(hll_key_in_db)

	hll := hyperloglog.DeserializeHLL(serialized_hll)

	if hll == nil {
		return -1, false
	}

	return hll.Estimate(), true
}

func (engine *DB) RemoveHyperLogLog(hll_key string) (success bool) {
	hll_key_in_db := TransformKeyToHLLKey(hll_key)
	return engine.Delete(hll_key_in_db)
}
