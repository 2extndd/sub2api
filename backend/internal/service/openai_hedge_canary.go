package service

const openAIHedgeCanaryBasisPointScale = 10_000

// OpenAIHedgeCanarySelected deterministically assigns one API-key/cohort pair
// to a bounded request cohort. The caller must never log or persist apiKeyID or
// the mixed value; both exist only for this in-memory decision.
func OpenAIHedgeCanarySelected(apiKeyID int64, cohort CohortKey, basisPoints int) bool {
	if apiKeyID <= 0 || basisPoints <= 0 || basisPoints > openAIHedgeCanaryBasisPointScale {
		return false
	}
	if basisPoints == openAIHedgeCanaryBasisPointScale {
		return true
	}

	value := uint64(apiKeyID) ^ (uint64(cohort) << 32) ^ 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	value ^= value >> 31

	return value%openAIHedgeCanaryBasisPointScale < uint64(basisPoints)
}
