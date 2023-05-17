// Package literal contains a selection of frequently used literals with GraphQL APIs
package literal

import "bytes"

var (
	COLON          = []byte(":")
	BANG           = []byte("!")
	LINETERMINATOR = []byte("\n")
	TAB            = []byte("	")
	SPACE          = []byte(" ")
	QUOTE          = []byte("\"")
	COMMA          = []byte(",")
	AT             = []byte("@")
	DOLLAR         = []byte("$")
	DOT            = []byte(".")
	SPREAD         = []byte("...")
	PIPE           = []byte("|")
	SLASH          = []byte("/")
	BACKSLASH      = []byte("\\")
	EQUALS         = []byte("=")
	SUB            = []byte("-")
	AND            = []byte("&")

	LPAREN        = []byte("(")
	RPAREN        = []byte(")")
	LBRACK        = []byte("[")
	RBRACK        = []byte("]")
	LBRACE        = []byte("{")
	DOUBLE_LBRACE = []byte("{{")
	DOUBLE_RBRACE = []byte("}}")
	RBRACE        = []byte("}")

	GOBOOL    = []byte("bool")
	GOINT32   = []byte("int32")
	GOFLOAT32 = []byte("float32")
	GOSTRING  = []byte("string")
	GONIL     = []byte("nil")

	EOF                           = []byte("eof")
	ID                            = []byte("ID")
	Date                          = []byte("Date")
	BOOLEAN                       = []byte("Boolean")
	STRING                        = []byte("String")
	INT                           = []byte("Int")
	FLOAT                         = []byte("Float")
	TYPE                          = []byte("type")
	UNDERSCORETYPE                = []byte("__type")
	UNDERSCORESCHEMA              = []byte("__schema")
	TYPENAME                      = []byte("__typename")
	GRAPHQLTYPE                   = []byte("graphqlType")
	INTERFACE                     = []byte("interface")
	INPUT                         = []byte("input")
	WASMFILE                      = []byte("wasmFile")
	INCLUDE                       = []byte("include")
	IF                            = []byte("if")
	SKIP                          = []byte("skip")
	SCHEMA                        = []byte("schema")
	EXTEND                        = []byte("extend")
	SCALAR                        = []byte("scalar")
	UNION                         = []byte("union")
	ENUM                          = []byte("enum")
	DIRECTIVE                     = []byte("directive")
	REPEATABLE                    = []byte("repeatable")
	QUERY                         = []byte("query")
	MUTATION                      = []byte("mutation")
	SUBSCRIPTION                  = []byte("subscription")
	IMPLEMENTS                    = []byte("implements")
	ON                            = []byte("on")
	FRAGMENT                      = []byte("fragment")
	NULL                          = []byte("null")
	OBJECT                        = []byte("object")
	DATA                          = []byte("data")
	URL                           = []byte("url")
	CONFIG_FILE_PATH              = []byte("configFilePath")
	CONFIG_STRING                 = []byte("configString")
	DELAY_SECONDS                 = []byte("delaySeconds")
	PIPELINE_CONFIG               = []byte("pipelineConfig")
	PIPELINE_CONFIG_STRING        = []byte("pipelineConfigString")
	PIPELINE_CONFIG_FILE          = []byte("pipelineConfigFile")
	TRANSFORMATION                = []byte("transformation")
	INPUT_JSON                    = []byte("inputJSON")
	DEFAULT_TYPENAME              = []byte("defaultTypeName")
	STATUS_CODE_TYPENAME_MAPPINGS = []byte("statusCodeTypeNameMappings")
	DOT_OBJECT_DOT                = []byte(".object.")
	DOT_ARGUMENTS_DOT             = []byte(".arguments.")
	ADDR                          = []byte("addr")
	ADD                           = []byte("add")
	BROKERADDR                    = []byte("brokerAddr")
	CLIENTID                      = []byte("clientID")
	TOPIC                         = []byte("topic")
	HOST                          = []byte("host")
	PARAMS                        = []byte("params")
	FIELD                         = []byte("field")
	BODY                          = []byte("body")
	METHOD                        = []byte("method")
	MODE                          = []byte("mode")
	HEADERS                       = []byte("headers")
	KEY                           = []byte("key")
	OP                            = []byte("op")
	REPLACE                       = []byte("replace")
	INITIAL_BATCH_SIZE            = []byte("initialBatchSize")
	MILLISECONDS                  = []byte("milliSeconds")
	PATH                          = []byte("path")
	VALUE                         = []byte("value")
	HTTP_METHOD_GET               = []byte("GET")
	HTTP_METHOD_POST              = []byte("POST")
	HTTP_METHOD_PUT               = []byte("PUT")
	HTTP_METHOD_DELETE            = []byte("DELETE")
	HTTP_METHOD_PATCH             = []byte("PATCH")

	TRUE  = []byte("true")
	FALSE = []byte("false")

	LocationQuery              = []byte("QUERY")
	LocationMutation           = []byte("MUTATION")
	LocationSubscription       = []byte("SUBSCRIPTION")
	LocationField              = []byte("FIELD")
	LocationFragmentDefinition = []byte("FRAGMENT_DEFINITION")
	LocationFragmentSpread     = []byte("FRAGMENT_SPREAD")
	LocationInlineFragment     = []byte("INLINE_FRAGMENT")
	LocationVariableDefinition = []byte("VARIABLE_DEFINITION")

	LocationSchema               = []byte("SCHEMA")
	LocationScalar               = []byte("SCALAR")
	LocationObject               = []byte("OBJECT")
	LocationFieldDefinition      = []byte("FIELD_DEFINITION")
	LocationArgumentDefinition   = []byte("ARGUMENT_DEFINITION")
	LocationInterface            = []byte("INTERFACE")
	LocationUnion                = []byte("UNION")
	LocationEnum                 = []byte("ENUM")
	LocationEnumValue            = []byte("ENUM_VALUE")
	LocationInputObject          = []byte("INPUT_OBJECT")
	LocationInputFieldDefinition = []byte("INPUT_FIELD_DEFINITION")
)

const (
	DOUBLE_LBRACE_STR = "{{"
	DOUBLE_RBRACE_STR = "}}"
)

type Literal []byte

func (l Literal) Equals(another Literal) bool {
	return bytes.Equal(l, another)
}

func CutBeforeSet(data []byte) (prefix []byte, ok bool) {
	setIndex := bytes.LastIndex(data, []byte("{set: "))
	if setIndex == -1 {
		prefix = data
		return
	}

	prefix = data[:setIndex]
	ok = true
	return
}
