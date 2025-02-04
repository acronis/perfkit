package search

import (
	"reflect"
	"testing"
)

func TestPageTokenPacking(t *testing.T) {
	var pts, ptd PageToken
	var s string
	var err error

	var eq = func(l, r PageToken) bool {
		return reflect.DeepEqual(l, r)
	}

	if s, err = pts.Pack(); err != nil {
		t.Error(err)
		return
	}

	if err = ptd.Unpack(s); err != nil {
		t.Error(err)
		return
	}

	if !eq(pts, ptd) {
		t.Error("tokens differ", pts, ptd)
		return
	}

	pts.Fields = []string{"id", "uuid"}

	if s, err = pts.Pack(); err != nil {
		t.Error(err)
		return
	}

	if err = ptd.Unpack(s); err != nil {
		t.Error(err)
		return
	}

	if !eq(pts, ptd) {
		t.Error("tokens differ", pts, ptd)
		return
	}

	pts.Offsets = map[string]int64{}
	pts.Offsets["default:1"] = 100500

	if s, err = pts.Pack(); err != nil {
		t.Error(err)
		return
	}

	if err = ptd.Unpack(s); err != nil {
		t.Error(err)
		return
	}

	if !eq(pts, ptd) {
		t.Error("tokens differ", pts, ptd)
		return
	}

	pts.Filter = map[string][]string{
		"xk": {"xv"},
		"yk": {"yv1, yv2"},
		"zk": {},
		"nk": nil,
	}

	if s, err = pts.Pack(); err != nil {
		t.Error(err)
		return
	}

	if err = ptd.Unpack(s); err != nil {
		t.Error(err)
		return
	}

	if !eq(pts, ptd) {
		t.Error("tokens differ", pts, ptd)
		return
	}
}

func TestCursorGen(t *testing.T) {
	var pt = PageToken{
		Filter: map[string][]string{
			"f1": {"1"},
		},
		Order: []string{"asc(f2)"},
	}

	var ctrl = newSelectCtrl(pt, 10, 0)

	if ctrl.Page.Limit != 10 || ctrl.Page.Offset != 0 {
		t.Error("wrong page", ctrl.Page)
		return
	}

	if len(ctrl.Order) != 1 || ctrl.Order[0] != "asc(f2)" {
		t.Error("wrong order", ctrl.Order)
		return
	}

	if len(ctrl.Where) != 1 || len(ctrl.Where["f1"]) != 1 || ctrl.Where["f1"][0] != "1" {
		t.Error("wrong where", ctrl.Where)
		return
	}
}
