package plan

import (
	"fmt"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/resolve"
	"golang.org/x/exp/slices"
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

		currentField.TransformRequired = true
		currentField.TransformDirective.Defined = true
		currentField.TransformDirective.ArrayWalked = currentField.Value.NodeKind() == resolve.NodeKindArray
		if value, ok := v.Operation.DirectiveArgumentValueByName(i, resolve.TransformArgGet); ok {
			for _, item := range strings.Split(v.Operation.ValueContentString(value), ".") {
				if item != "[]" && item != "" {
					currentField.TransformDirective.Get = append(currentField.TransformDirective.Get, item)
				}
			}
		}
		v.resolveTransformForChildren(currentField.Value, &currentField.TransformDirective, 0)
		if value, ok := v.Operation.DirectiveArgumentValueByName(i, resolve.TransformArgMath); ok {
			if !currentField.TransformDirective.ArrayWalked {
				v.Walker.StopWithInternalErr(fmt.Errorf("@transform with math can only be used on arrays"))
				return
			}
			transformMath := resolve.TransformMath(v.Operation.ValueContentString(value))
			transformGetNodeKind := currentField.TransformDirective.GetNode.NodeKind()
			switch transformMath {
			case resolve.TransformMathMax, resolve.TransformMathMin, resolve.TransformMathSum, resolve.TransformMathAvg:
				if transformGetNodeKind != resolve.NodeKindInteger && transformGetNodeKind != resolve.NodeKindFloat {
					v.Walker.StopWithInternalErr(fmt.Errorf("@transform math [%s] can only be used on integer or float values", transformMath))
					return
				}
			case resolve.TransformMathFirst, resolve.TransformMathLast, resolve.TransformMathCount:
			default:
				v.Walker.StopWithInternalErr(fmt.Errorf("not support transform math [%s]", transformMath))
				return
			}
			currentField.TransformDirective.Math = transformMath
		}
		return
	}
}

func (v *Visitor) resolveTransformForChildren(fieldValue resolve.Node, transform *resolve.TransformDirective, pathIndex int) bool {
	if pathIndex == len(transform.Get) {
		transform.GetNode = fieldValue
		return false
	}

	switch ret := fieldValue.(type) {
	case *resolve.Array:
		transform.ArrayWalked = true
		ret.TransformItemRequired = v.resolveTransformForChildren(ret.Item, transform, pathIndex) || ret.TransformItemRequired
		return true
	case *resolve.Object:
		if ret.TransformFieldRequired {
			v.Walker.StopWithInternalErr(fmt.Errorf("repeat path [%s] @transform", strings.Join(transform.Get[:pathIndex+1], ".")))
			return false
		}
		fieldName := transform.Get[pathIndex]
		for i := range ret.Fields {
			if string(ret.Fields[i].Name) == fieldName {
				pathIndex++
				ret.TransformFieldIndex = i
				ret.TransformFieldRequired = true
				ret.Fields[i].TransformRequired = v.resolveTransformForChildren(ret.Fields[i].Value, transform, pathIndex) || ret.Fields[i].TransformRequired
				return true
			}
		}
		v.Walker.StopWithInternalErr(fmt.Errorf("invalid path [%s] @transform", strings.Join(transform.Get[:pathIndex+1], ".")))
		return false
	case *resolve.String:
		if slices.Contains(ret.Path, resolve.QueryRawKey) {
			ret.TransformFieldName = transform.Get[pathIndex]
			transform.ArrayWalked = !ret.FirstRawResult
			return true
		}
	}
	v.Walker.StopWithInternalErr(fmt.Errorf("invalid nodeKind [%d] on path [%s] @transform", fieldValue.NodeKind(), strings.Join(transform.Get[:pathIndex], ".")))
	return false
}
