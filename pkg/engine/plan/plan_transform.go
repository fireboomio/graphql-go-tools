package plan

import (
	"github.com/wundergraph/graphql-go-tools/pkg/engine/resolve"
	"strings"
)

func (v *Visitor) resolveTransform(ref int) {
	index, ok := v.currentFieldIndexes[ref]
	if !ok || index >= len(v.currentFields) {
		return
	}

	currentField := v.currentFields[index].popField
	for _, i := range v.Operation.Fields[ref].Directives.Refs {
		if v.Operation.DirectiveNameString(i) != "transform" {
			continue
		}

		currentField.TransformDirective.Defined = true
		if value, ok := v.Operation.DirectiveArgumentValueByName(i, resolve.TransformArgGet); ok {
			for _, item := range strings.Split(v.Operation.ValueContentString(value), ".") {
				if item != "[]" {
					currentField.TransformDirective.Get = append(currentField.TransformDirective.Get, item)
				}
			}
		}
		if value, ok := v.Operation.DirectiveArgumentValueByName(i, resolve.TransformArgMath); ok {
			currentField.TransformDirective.Math = resolve.TransformMath(v.Operation.ValueContentString(value))
		}
		currentField.TransformDirective.GetNode = v.searchValueNode(currentField.Value, currentField.TransformDirective.Get)
	}
}

func (v *Visitor) searchValueNode(fieldValue resolve.Node, path []string) resolve.Node {
	if len(path) == 0 {
		return fieldValue
	}
	switch ret := fieldValue.(type) {
	case *resolve.Array:
		return v.searchValueNode(ret.Item, path)
	case *resolve.Object:
		for i := range ret.Fields {
			if string(ret.Fields[i].Name) == path[0] {
				return v.searchValueNode(ret.Fields[i].Value, path[1:])
			}
		}
		return &resolve.Null{}
	default:
		return fieldValue
	}
}
