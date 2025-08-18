package search

func cutSourcesByOffsets[T Searchable[T]](sources map[string]Searcher[T], offsets map[string]int64) map[string]Searcher[T] {
	if offsets == nil {
		return sources
	}

	for src := range sources {
		if _, ok := offsets[src]; !ok {
			delete(sources, src)
		}
	}

	return sources
}

// nolint:unparam TODO: Add more error handling?
func createNextOffsetBasedPageToken(previousPageToken PageToken, usedFromSources map[string]int64) *PageToken {
	if usedFromSources == nil {
		return nil
	}

	return &PageToken{
		Fields:  previousPageToken.Fields,
		Offsets: usedFromSources,
		Filter:  previousPageToken.Filter,
		Order:   previousPageToken.Order,
	}
}
