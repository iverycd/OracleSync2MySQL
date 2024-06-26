package connect

// DbConnStr related with config.yml
type DbConnStr struct {
	SrcHost      string
	SrcUserName  string
	SrcPassword  string
	SrcDatabase  string
	SrcPort      int
	SrcSchema    string
	DestHost     string
	DestPort     int
	DestUserName string
	DestPassword string
	DestDatabase string
	DestParams   map[string]string
}
