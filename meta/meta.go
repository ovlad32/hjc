package meta

type TableMeta struct {
	ID         string
	DatabaseID string
	SchemaName string
	TableName  string
	RowCount   int
}

type ColumnMeta struct {
	ID                  string
	TableID             string
	ColumnName          string
	Position            int
	DataType            string
	IsNullDataType      bool
	DataLength          int
	IsNullDataLength    bool
	DataPrecision       int
	IsNullDataPrecision bool
	DataScale           int
	IsNullDataScale     bool
	UniqueCount         int
	EmptyCount          int
}
