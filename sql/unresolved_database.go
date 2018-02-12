package sql

type UnresolvedDatabase struct{}

func (d *UnresolvedDatabase) Name() string {
	return "unresolved_database"
}

func (d *UnresolvedDatabase) Tables() map[string]Table {
	return make(map[string]Table)
}

func (d *UnresolvedDatabase) Table(name string) (Table, error) {
	return nil, nil
}
