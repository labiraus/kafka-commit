package data

const (
	PropStruct  = "struct"
	PropBool    = "bool"
	PropInt     = "int"
	PropInt64   = "int64"
	PropFloat32 = "float32"
	PropFloat64 = "float64"
	PropString  = "string"
	PropTime    = "time"
)

const (
	ValidGreaterThan0 = "greater than 0"
	ValidNotBlank     = "not blank"
	ValidMandatory    = "mandatory"
	ValidOptional     = "optional"
)

const (
	ConsequenceEmail = "email"
	ConsequenceApi   = "api call"
)

type Validator struct {
	Topic       string
	Consequence string
	Endpoint    string
	Template    string
	Properties  []Property
}

type Property struct {
	Name     string
	Type     string
	Optional string
	Children []Property
}
