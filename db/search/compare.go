package search

import (
	"fmt"

	"github.com/acronis/perfkit/db"
)

type comparator[T Searchable[T]] func(a, b T) bool
type comparators[T Searchable[T]] map[string]map[string]comparator[T]

func makeComparator[T Searchable[T]](values []string, comparable comparators[T]) (comparator[T], error) {
	var less func(a, b T) bool
	if len(values) == 0 {
		return less, nil
	}

	var finalLess func(a, b T) bool

	for i := len(values) - 1; i >= 0; i-- {
		value := values[i]

		fnc, field, err := db.ParseFunc(value)
		if err != nil {
			return nil, err
		}

		if fnc == "" {
			return nil, fmt.Errorf("empty order function")
		}

		if field == "" {
			return nil, fmt.Errorf("empty order field")
		}

		fieldComparators, ok := comparable[field]
		if !ok {
			return nil, fmt.Errorf("bad order field '%v'", field)
		}

		less, ok := fieldComparators[fnc]
		if !ok {
			return nil, fmt.Errorf("bad order function '%v'", fnc)
		}

		if finalLess == nil {
			finalLess = less
		} else {
			var deepLess = finalLess

			finalLess = func(a, b T) bool {
				if less(a, b) {
					return true
				} else if less(b, a) {
					return false
				}

				return deepLess(a, b)
			}
		}
	}

	return finalLess, nil
}
