package search

import (
	"fmt"

	"github.com/acronis/perfkit/db"
)

// CursorIterable is an interface for entities that can be iterated by cursor
type CursorIterable[T any] interface {
	Unique(field string) bool
	Nullable(field string) bool
	Cursor(field string) (string, error)
}

type sorting struct {
	Field string
	Func  string
}

func uniqueSort[T CursorIterable[T]](encodedSorts []string, cursors map[string]string, instance T) ([]string, []sorting, error) {
	var hasUniqueSorting = false
	var uniqueOrderDirection int

	var encoded []string
	var sorts []sorting

	for _, v := range encodedSorts {
		var fnc, field, err = db.ParseFunc(v)
		if err != nil {
			return nil, nil, err
		}

		var unique = instance.Unique(field)
		var nullable = instance.Nullable(field)
		hasUniqueSorting = unique && !nullable

		encoded = append(encoded, v)
		sorts = append(sorts, sorting{
			Field: field,
			Func:  fnc,
		})

		switch fnc {
		case "asc":
			uniqueOrderDirection++
		case "desc":
			uniqueOrderDirection--
		}

		if unique {
			if !nullable {
				break
			} else if cursors != nil {
				if val, ok := cursors[field]; ok && val != db.SpecialConditionIsNull {
					if fnc != "desc" {
						break
					}
				}
			}
		}
	}

	if !hasUniqueSorting {
		if uniqueOrderDirection >= 0 {
			encoded = append(encoded, "asc(id)")
			sorts = append(sorts, sorting{Field: "id", Func: "asc"})
		} else {
			encoded = append(encoded, "desc(id)")
			sorts = append(sorts, sorting{Field: "id", Func: "desc"})
		}
	}

	return encoded, sorts, nil
}

func orderCondition(val, fnc string) (expr string, flag bool, err error) {
	var direction string
	switch fnc {
	case "asc":
		switch val {
		case db.SpecialConditionIsNull:
			return db.SpecialConditionIsNotNull, false, nil
		case db.SpecialConditionIsNotNull:
			return "", true, nil
		default:
			direction = "gt"
		}
	case "desc":
		switch val {
		case db.SpecialConditionIsNotNull:
			return db.SpecialConditionIsNull, false, nil
		case db.SpecialConditionIsNull:
			return "", true, nil
		default:
			direction = "lt"
		}
	default:
		return "", false, fmt.Errorf("missing ordering for cursor")
	}

	return fmt.Sprintf("%s(%v)", direction, val), false, nil
}

func splitQueryOnLightWeightQueries[T CursorIterable[T]](pt PageToken, instance T) ([]PageToken, error) {
	var tokens []PageToken

	if len(pt.Fields) == 0 {
		tokens = append(tokens, pt)
		return tokens, nil
	}

	// check for unique sorting
	var encodedSorts, sorts, err = uniqueSort(pt.Order, pt.Cursor, instance)
	if err != nil {
		return nil, err
	}

	if len(pt.Cursor) == 0 {
		pt.Order = encodedSorts
		tokens = append(tokens, pt)
		return tokens, nil
	}

	// construct sort map for fast access
	var orderFunctions = map[string]string{}
	for _, sort := range sorts {
		orderFunctions[sort.Field] = sort.Func
	}

	// add condition based on cursor
	var whereFromCursor = func(fld, val string, pt *PageToken) (bool, error) {
		var filter, empty, filterErr = orderCondition(val, orderFunctions[fld])
		if filterErr != nil {
			return false, filterErr
		}

		if empty {
			return true, nil
		}

		pt.Filter[fld] = append(pt.Filter[fld], filter)
		return false, nil
	}

	for cursor := range pt.Cursor {
		if _, ok := orderFunctions[cursor]; !ok {
			return nil, fmt.Errorf("prohibited cursor, not mentioned it order: %v", cursor)
		}
	}

	// split to x page tokens
	for i := range sorts {
		var cpt = pt
		var last = len(sorts) - 1 - i

		// copy filters
		cpt.Filter = make(map[string][]string, len(sorts)-1-i)
		for k, v := range pt.Filter {
			cpt.Filter[k] = v
		}

		// add equal condition on all fields except last in sorts
		for j := 0; j <= last-1; j++ {
			var fld = sorts[j].Field
			var val = pt.Cursor[fld]

			cpt.Filter[fld] = append(cpt.Filter[fld], val)
		}

		// add gt / lt condition for last sorting
		var empty bool
		if val, ok := cpt.Cursor[sorts[last].Field]; ok {
			if empty, err = whereFromCursor(sorts[last].Field, val, &cpt); err != nil {
				return nil, err
			}
		} else {
			continue
		}

		if empty {
			continue
		}

		// Add only needed sort to cpt
		cpt.Order = []string{}
		for j := last; j <= len(sorts)-1; j++ {
			cpt.Order = append(cpt.Order, encodedSorts[j])

			var sortField = sorts[j].Field

			if instance.Unique(sortField) {
				if !instance.Nullable(sortField) {
					break
				}

				var becomeUnique = false
				// for ASC if we have a value, that means we already select all null rows
				// for DESC Nulls can start at any row
				if sorts[j].Func == "asc" {
					for _, val := range cpt.Filter[sortField] {
						if val != db.SpecialConditionIsNull {
							becomeUnique = true
							break
						}
					}
				}
				if becomeUnique {
					break
				}
			}
		}

		cpt.Cursor = nil

		tokens = append(tokens, cpt)
	}

	return tokens, nil
}

func createNextCursorBasedPageToken[T CursorIterable[T]](previousPageToken PageToken, items []T, limit int64, instance T) (*PageToken, error) {
	if int64(len(items)) < limit {
		return nil, nil
	}

	var pt PageToken
	pt.Cursor = make(map[string]string)
	pt.Fields = previousPageToken.Fields

	var encoded, sorts, err = uniqueSort(previousPageToken.Order, previousPageToken.Cursor, instance)
	if err != nil {
		return nil, err
	}
	pt.Order = encoded

	var last = items[len(items)-1]
	for _, sort := range sorts {
		var value string
		if value, err = last.Cursor(sort.Field); err != nil {
			return nil, err
		}
		pt.Cursor[sort.Field] = value
	}

	return &pt, nil
}
