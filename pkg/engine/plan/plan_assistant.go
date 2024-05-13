package plan

import (
	"github.com/wundergraph/graphql-go-tools/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/resolve"
	"golang.org/x/exp/slices"
)

func (v *Visitor) resetWaitExportedRequired(ref int) {
	index, ok := v.currentFieldIndexes[ref]
	if !ok || index >= len(v.currentFieldIndexes) {
		return
	}

	currentField := v.currentFields[index].popField
	if currentField.NoneExportedBefore || currentField.WaitExportedRequired {
		return
	}

	var itemWaitFuncs []func(*resolve.Context) bool
	for _, item := range *v.currentFields[index].fields {
		if item.WaitExportedRequired {
			currentField.WaitExportedRequired = true
			return
		}
		if item.WaitExportedRequiredFunc != nil {
			itemWaitFuncs = append(itemWaitFuncs, item.WaitExportedRequiredFunc)
		}
	}
	if len(itemWaitFuncs) == 0 {
		return
	}

	currentWaitFunc := currentField.WaitExportedRequiredFunc
	currentField.WaitExportedRequiredFunc = func(ctx *resolve.Context) bool {
		return currentWaitFunc != nil && currentWaitFunc(ctx) ||
			slices.ContainsFunc(itemWaitFuncs, func(f func(*resolve.Context) bool) bool { return f(ctx) })
	}
	return
}

func (v *Visitor) resolveFormatDateTime(fieldRef int) *resolve.DateTimeFormat {
	if !v.Operation.Fields[fieldRef].HasDirectives {
		return nil
	}

	for _, ref := range v.Operation.Fields[fieldRef].Directives.Refs {
		if v.Operation.Input.ByteSliceString(v.Operation.Directives[ref].Name) != "formatDateTime" {
			continue
		}

		formatValue, _ := v.Operation.DirectiveArgumentValueByName(ref, []byte("format"))
		customFormatValue, _ := v.Operation.DirectiveArgumentValueByName(ref, []byte("customFormat"))
		if formatValue.Kind == ast.ValueKindEnum || customFormatValue.Kind == ast.ValueKindString {
			return &resolve.DateTimeFormat{
				Format:       v.Operation.StringValueContentString(formatValue.Ref),
				CustomFormat: v.Operation.StringValueContentString(customFormatValue.Ref),
			}
		}
	}
	return nil
}
