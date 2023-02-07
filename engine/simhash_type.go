package engine

import sh "github.com/MomirMilutinovic/NAiSP_Projekat/SimHash"

func TransformKeyToSimhashKey(key string) string {
	return "simhash." + key
}

func (engine *DB) CreateSimhash(key string, text string) (success bool) {
	simhash := sh.NewSimHash(text)
	serialized_simhash := simhash.Serialize()

	return engine.Put(TransformKeyToSimhashKey(key), serialized_simhash)
}

func (engine *DB) SimhashGetFingerprint(key string) (fingerprint string, simhash_exists bool) {
	serialized_simhash := engine.Get(TransformKeyToSimhashKey(key))

	if serialized_simhash != nil {
		deserialized_simhash := sh.NewSimHash("")
		bool := deserialized_simhash.Deserialize(serialized_simhash)
		if bool {
			return deserialized_simhash.GetFingerprint(), true
		} else {
			return "", false
		}
	} else {
		return "", false
	}
}

func (engine *DB) SimhashUpdateText(key string, new_text string) bool {
	serialized_simhash := engine.Get(TransformKeyToSimhashKey(key))

	if serialized_simhash != nil {
		deserialized_simhash := sh.NewSimHash("")
		bool := deserialized_simhash.Deserialize(serialized_simhash)
		if bool {
			deserialized_simhash.NewText(new_text)
			serialized_simhash = deserialized_simhash.Serialize()
			return engine.Put(TransformKeyToSimhashKey(key), serialized_simhash)
		} else {
			return false
		}
	} else {
		return false
	}
}

func (engine *DB) SimhashCalculateDistance(key1 string, key2 string) (distance int, simhashes_exist bool) {
	serialized_simhash1 := engine.Get(TransformKeyToSimhashKey(key1))
	serialized_simhash2 := engine.Get(TransformKeyToSimhashKey(key2))

	if serialized_simhash1 != nil && serialized_simhash2 != nil {
		deserialized_simhash1 := sh.NewSimHash("")
		deserialized_simhash2 := sh.NewSimHash("")
		bool := deserialized_simhash1.Deserialize(serialized_simhash1)
		if !bool {
			return -1, false
		}
		bool = deserialized_simhash2.Deserialize(serialized_simhash2)
		if !bool {
			return -1, false
		}

		distance := deserialized_simhash1.CalculateDistance(deserialized_simhash2)
		return distance, true

	} else {
		return -1, false
	}
}

func (engine *DB) SimhashCalculateSimilarity(distance int) (similarity float64) {
	return sh.CalculateSimilarity(distance)
}

func (engine *DB) DeleteSimhash(key string) (success bool) {
	return engine.Delete(TransformKeyToSimhashKey(key))
}
