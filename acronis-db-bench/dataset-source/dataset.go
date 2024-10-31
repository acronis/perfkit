package dataset_source

type DataSetSource interface {
	// GetColumnNames returns the names of the columns in the dataset.
	GetColumnNames() []string

	// GetNextRow returns the next row from the dataset.
	GetNextRow() ([]interface{}, error)

	// Close closes the dataset source.
	Close()
}
