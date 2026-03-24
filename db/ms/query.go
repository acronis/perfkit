package ms

import "github.com/acronis/perfkit/db"

// @cpt-perfkit-db-dod-ms-adapter-unsupported-ops

func (g *msGateway) Exec(format string, args ...interface{}) (db.Result, error) {
	return nil, nil
}

func (g *msGateway) QueryRow(format string, args ...interface{}) db.Row {
	return &db.EmptyRows{}
}

func (g *msGateway) Query(format string, args ...interface{}) (db.Rows, error) {
	return &db.EmptyRows{}, nil
}
