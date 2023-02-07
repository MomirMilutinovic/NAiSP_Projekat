package engine

//Datoteka implementira metode za rukovanje cms-om putem kljuceva iz baze
import "github.com/MomirMilutinovic/NAiSP_Projekat/CMS"

// Dodavanje CMS-a u bazu
func (engine *DB) PutCMS(key string, cms CMS.CMS) bool {
	key = "cms." + key
	return engine.Put(key, cms.Serialize())
}

// Citanje cms-a iz baze
func (engine *DB) GetCMS(cmsKey string) *CMS.CMS {
	return CMS.Deserialize(engine.Get("cms." + cmsKey))
}

// Dodavanje u cms koji je u bazi, putem kljuca
func (engine *DB) AddToCMS(cmsKey string, key []byte) int {
	cms := engine.GetCMS(cmsKey)
	if cms == nil {
		return -1
	}
	cms.Add(key)
	engine.PutCMS(cmsKey, *cms)
	return 0
}

// Citanje iz cms-a koji je u bazi, putem kljuca, vraca se statusna promenljiva tipa int i promenljiva tipa uint koja je povratna vrednost cms-a
func (engine *DB) ReadFromCMS(cmsKey string, key []byte) (int, uint) {
	cms := engine.GetCMS(cmsKey)
	if cms == nil {
		return -1, 0
	}
	return 0, cms.Read(key)
}
func (engine *DB) DeleteCMS(cmsKey string) bool {
	return engine.Delete("cms." + cmsKey)
}
func (engine *DB) CreateCMS(key string, precision float64, certainty float64) (success bool) {

	if precision <= 0 || certainty <= 0 || precision >= 1 || certainty >= 1 {
		return false
	}
	cms := CMS.CMS{}
	cms.Init(precision, certainty)
	return engine.PutCMS(key, cms)
}
