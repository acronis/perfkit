package search

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/acronis/perfkit/db"
)

func id2str(id int64) string {
	return fmt.Sprintf("%v", id)
}

func str2id(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

func int64Min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// Comparable is an interface for entities that can be compared
type Comparable[T any] interface {
	Less(T) bool
	Equal(T) bool
}

type Searchable[T any] interface {
	Comparable[T]
	CursorIterable[T]
}

type Searcher[T any] interface {
	Search(*db.SelectCtrl) ([]T, int64, error)
}

type loadedPage[T Searchable[T]] struct {
	Items     []T
	Size      int64
	Timestamp string
}

type searchEngine[T Searchable[T]] struct {
	lods         map[string][]string
	comparators  map[string]map[string]comparator[T]
	equalFunc    comparator[T]
	loadFunc     func(Searcher[T], *db.SelectCtrl) ([]T, int64, error)
	uniqueFields map[string]bool
	cursorGen    func(entity *T, field string) (string, error)
}

func cleanSources[T Searchable[T]](sources map[string]Searcher[T]) map[string]Searcher[T] {
	for key, source := range sources {
		if source == nil {
			delete(sources, key)
		}
	}

	return sources
}

func (e *searchEngine[T]) search(sources map[string]Searcher[T], pt PageToken, limit int64, cursorPagingEnabled bool, instance T) (*loadedPage[T], *PageToken, error) {
	var finalPage = loadedPage[T]{Timestamp: db.FormatTimeStamp(time.Now())}

	sources = cleanSources(sources)
	if len(sources) == 0 {
		return &finalPage, nil, nil
	}

	var sorts []string
	if cursorPagingEnabled {
		if encoded, _, sortsErr := uniqueSort(pt.Order, pt.Cursor, instance); sortsErr != nil {
			return nil, nil, db.BadInputError{Details: fmt.Errorf("unique sort creation error: %v", sortsErr)}
		} else {
			sorts = encoded
		}
	} else {
		sorts = pt.Order
	}

	var less, cmpErr = makeComparator(sorts, e.comparators)
	if cmpErr != nil {
		return nil, nil, db.BadInputError{Details: fmt.Errorf("creation cmp error: %v", cmpErr)}
	}

	var pts []PageToken
	var offsets map[string]int64
	if cursorPagingEnabled {
		var err error
		if pts, err = splitQueryOnLightWeightQueries(pt, instance); err != nil {
			return nil, nil, db.BadInputError{Details: fmt.Errorf("cursor modify error: %v", err)}
		}
	} else {
		pts = append(pts, pt)
		offsets = pt.Offsets
		sources = cutSourcesByOffsets(sources, offsets)
	}

	var finalList = []T{}
	var finalCount int64
	for _, cpt := range pts {
		var pages, counts, err = e.load(sources, cpt, limit)
		if err != nil {
			return nil, nil, err
		}

		if len(cpt.Fields) == 0 {
			for _, count := range counts {
				finalCount += count
			}
			continue
		}

		pages = cleanPages(pages)
		if less == nil && len(pages) > 1 {
			return nil, nil, db.BadInputError{Details: fmt.Errorf("failed to merge multiple pages, order is empty: %v", err)}
		}
		finalList, offsets = e.merge(finalList, pages, limit, less, offsets)

		if len(finalList) >= int(limit) {
			break
		}
	}

	if len(pt.Fields) == 0 {
		finalPage.Size = finalCount
		return &finalPage, nil, nil
	}

	var nextPageToken *PageToken
	var err error
	if cursorPagingEnabled {
		if nextPageToken, err = createNextCursorBasedPageToken(pt, finalList, limit, instance); err != nil {
			return nil, nil, db.BadInputError{Details: fmt.Errorf("creation cmp error: %v", err)}
		}
	} else {
		nextPageToken = createNextOffsetBasedPageToken(pt, offsets)
	}

	finalPage.Items = finalList
	finalPage.Size = int64(len(finalPage.Items))

	return &finalPage, nextPageToken, nil
}

type loadResult[T Searchable[T]] struct {
	key   string
	items []T
	count int64
	err   error
}

func (e *searchEngine[T]) load(sources map[string]Searcher[T], pt PageToken, limit int64) (pages map[string][]T, counts map[string]int64, err error) {
	sources = cleanSources(sources)

	pages = map[string][]T{}
	counts = map[string]int64{}
	if len(sources) == 0 {
		return nil, nil, nil
	}

	if len(sources) == 1 {
		for key, source := range sources {
			var offset = pt.Offsets[key]
			var ctrl = newSelectCtrl(pt, limit, offset)
			var items, count, err = e.loadFunc(source, ctrl)
			if err != nil {
				return nil, nil, err
			}
			pages[key] = items
			counts[key] = count
			return pages, counts, nil
		}
	}

	var results = make([]loadResult[T], len(sources))
	var wg sync.WaitGroup
	wg.Add(len(sources))

	var i = 0
	for k, s := range sources {
		go func(key string, source Searcher[T], index int) {
			var offset = pt.Offsets[key]
			var ctrl = newSelectCtrl(pt, limit, offset)

			var result = loadResult[T]{key: key}
			result.items, result.count, result.err = e.loadFunc(source, ctrl)
			results[index] = result

			wg.Done()
		}(k, s, i)

		i++
	}
	wg.Wait()

	for _, result := range results {
		if result.err != nil {
			return nil, nil, result.err
		}

		// nolint:gocritic // TODO: >= is suspicious here. should be > ?
		if len(result.items) >= 0 {
			pages[result.key] = result.items
			counts[result.key] = result.count
		}
	}

	return pages, counts, nil
}

type pageToMerge[T Searchable[T]] struct {
	key   string
	items []T
	used  int64
}

// Not safe
func (p *pageToMerge[T]) headPlus(index int64) T {
	return p.items[p.used+index]
}

func (p *pageToMerge[T]) isSafeHeadPlus(index int64) bool {
	return p.used+index < int64(len(p.items))
}

func cleanPages[T Searchable[T]](pages map[string][]T) map[string][]T {
	for key, page := range pages {
		if len(page) == 0 {
			delete(pages, key)
		}
	}

	return pages
}

// nolint:funlen // TODO: Try to split this function
func (e *searchEngine[T]) merge(finalList []T, pages map[string][]T, limit int64, less comparator[T], offsets map[string]int64) ([]T, map[string]int64) {
	pages = cleanPages(pages)

	if len(pages) == 0 {
		return finalList, nil
	}

	var usedTombstones = map[string]int64{}
	var pagesToMerge []pageToMerge[T]
	for key, page := range pages {
		pagesToMerge = append(pagesToMerge, pageToMerge[T]{
			key:   key,
			items: page,
			used:  0,
		})
	}

	// Algorithm explanation
	//
	// At the beginning we have
	// 1. Final list
	// 2. Multiple (or single) arrays in any order
	//
	// Elements in each array are sorted in 'less' order.
	// Assume 'less' order is just asc(id).
	// So, may have:
	//
	// Final list:
	// 1 - 2 - 3 - 4
	//
	// Pages to merge:
	// 7 -  8 - 9
	// 9 - 10 - 12
	// 5 -  6 - 11
	//
	// Limit: 7
	//
	// Initial finalList that we have can be empty or already have some elements.

	for len(pagesToMerge) > 0 && int64(len(finalList)) < limit {
		// If there is only one page, append it without merge
		// Final list: 1 - 2 - 3 - 4
		// Page to merge: 5 -  6 - 7
		// Limit: 7
		//
		// Final list: 1 - 2 - 3 - 4 - 5 - 6 - 7
		if len(pagesToMerge) == 1 {
			var numberOfItemsToTake = int64Min(limit-int64(len(finalList)), int64(len(pagesToMerge[0].items))-pagesToMerge[0].used)
			finalList = append(finalList, pagesToMerge[0].items[pagesToMerge[0].used:pagesToMerge[0].used+numberOfItemsToTake]...)
			pagesToMerge[0].used += numberOfItemsToTake
			break
		}

		// If there are multiple pages, execute invariant of loop.
		// Invariant of loop: all loaded pages heads have to be arranged in order defined by function ctor.less(a, b).
		// Head is page.items[p.used]
		// (i) - means head
		// If it was:
		// Pages to merge:
		// (7) -  8 - 9
		// (9) - 10 - 12
		// (5) -  6 - 11
		//
		// We should make:
		// Pages to merge:
		// (5) -  6 - 11
		// (7) -  8 - 9
		// (9) - 10 - 12
		//
		// Sort to invariant condition:
		var swapped = true
		for swapped {
			swapped = false
			for i := 1; i < len(pagesToMerge); i++ {
				if less(pagesToMerge[i].headPlus(0), pagesToMerge[i-1].headPlus(0)) {
					pagesToMerge[i], pagesToMerge[i-1] = pagesToMerge[i-1], pagesToMerge[i]
					swapped = true
				}
			}
		}

		// Most of the logic will be executed only with 0 and 1 pages in the list, so let's define them.
		var first = pagesToMerge[0]
		var second = pagesToMerge[1]

		// We should search minimal element from first page, larger than the head from the second one
		// (i) - means head
		// If it was:
		// Pages to merge:
		// (5) -  6 - 11
		// (7) -  8 - 9
		// (9) - 10 - 12
		//
		// We should make:
		// Pages to merge:
		//  5  -  6 - (11)
		// (7) -  8 - 9
		// (9) - 10 - 12
		var numberOfItemsInFinalList = int64(len(finalList))
		var numberOfItemsToTake int64 = 0
		for numberOfItemsInFinalList+numberOfItemsToTake < limit &&
			first.isSafeHeadPlus(numberOfItemsToTake) &&
			less(first.headPlus(numberOfItemsToTake), second.headPlus(0)) {
			numberOfItemsToTake++
		}

		// We should search equal elements from all pages to merge and deduplicate them
		// Here is very important assumption:
		// Duplicates can be presented not more than in two pages.
		//
		// (i) - means head
		// If it was:
		// Pages to merge:
		//  7  -  8 - (9)
		// (9) - 10 - 12
		//  5  -  6 - (11)
		//
		// We should make:
		// Pages to merge:
		// 7  -  8  -   9  - ()
		// 9 - (10) -  12
		// 5  -  6  - (11)
		for numberOfItemsInFinalList+numberOfItemsToTake < limit &&
			first.isSafeHeadPlus(numberOfItemsToTake) &&
			second.isSafeHeadPlus(0) &&
			e.equalFunc(first.headPlus(numberOfItemsToTake), second.headPlus(0)) {
			numberOfItemsToTake++
			second.used++
		}

		// Not same, not less and not bigger
		// Increment numberOfItemsToTake anyway to avoid infinite loop
		if numberOfItemsToTake == 0 {
			numberOfItemsToTake++
		}

		// Append the smallest items to final page
		// (i) - means head
		// If it was:
		// Final list: 1 - 2 - 3 - 4
		// Pages to merge:
		//  5  -  6 - (11)
		// (7) -  8 - 9
		// (9) - 10 - 12
		//
		// We should make:
		// Final list: 1 - 2 - 3 - 4 - 5 - 6
		// Pages to merge:
		//  5  -  6 - (11)
		// (7) -  8 - 9
		// (9) - 10 - 12
		finalList = append(finalList, first.items[first.used:first.used+numberOfItemsToTake]...)
		first.used += numberOfItemsToTake
		pagesToMerge[0] = first
		pagesToMerge[1] = second

		// Drop pages to merge with all used elements
		// (i) - means head
		// If it was:
		// Pages to merge:
		// 7  -  8  -   9  - ()
		// 9 - (10) -  12
		// 5  -  6  - (11)
		//
		// We should make:
		// Pages to merge:
		// 9 - (10) -  12
		// 5  -  6  - (11)
		for i := 0; i < len(pagesToMerge); i++ {
			if pagesToMerge[i].isSafeHeadPlus(0) {
				continue
			}
			for j := i + 1; j < len(pagesToMerge); j++ {
				pagesToMerge[j], pagesToMerge[j-1] = pagesToMerge[j-1], pagesToMerge[j]
			}

			var pageToRemove = pagesToMerge[len(pagesToMerge)-1]
			if pageToRemove.used == limit {
				usedTombstones[pageToRemove.key] = pageToRemove.used
			}

			pagesToMerge = pagesToMerge[:len(pagesToMerge)-1]
			i--
		}
	}

	var lastMergeOffsets map[string]int64
	for _, page := range pagesToMerge {
		if page.used == int64(len(page.items)) && int64(len(page.items)) < limit {
			continue
		}
		if lastMergeOffsets == nil {
			lastMergeOffsets = map[string]int64{}
		}
		lastMergeOffsets[page.key] = page.used
	}

	for src, offset := range usedTombstones {
		if lastMergeOffsets == nil {
			lastMergeOffsets = map[string]int64{}
		}
		lastMergeOffsets[src] = offset
	}

	if offsets != nil {
		for src := range lastMergeOffsets {
			if used, ok := offsets[src]; ok {
				lastMergeOffsets[src] += used
			}
		}
	}
	offsets = lastMergeOffsets

	return finalList, offsets
}
