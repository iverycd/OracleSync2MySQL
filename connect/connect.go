package connect

type DbConnStr struct {
	SrcHost      string
	SrcUserName  string
	SrcPassword  string
	SrcDatabase  string
	SrcPort      int
	DestHost     string
	DestPort     int
	DestUserName string
	DestPassword string
	DestDatabase string
}
