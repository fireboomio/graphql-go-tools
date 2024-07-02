package plan

import (
	"github.com/wundergraph/graphql-go-tools/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/resolve"
	"golang.org/x/exp/slices"
)

func (v *Visitor) SetWaitExportedRequiredForVariable() {
	if v.currentField.NoneExportedBefore || v.currentField.WaitExportedRequired {
		return
	}

	fetch, ok := v.currentField.Value.(resolve.FieldFetchVariable)
	if !ok {
		return
	}

	for _, item := range fetch.FetchedVariables() {
		if _, found := v.exportedVariables[item]; found {
			v.currentField.WaitExportedRequired = true
			return
		}
	}
}

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

const (
	formatDateTimeDirective       = "formatDateTime"
	formatDateTimeArgFormat       = "format"
	formatDateTimeArgCustomFormat = "customFormat"
)

func (v *Visitor) resolveDateFormatArguments(fieldRef int) map[string]string {
	if !v.Operation.Fields[fieldRef].HasDirectives {
		return nil
	}

	for _, ref := range v.Operation.Fields[fieldRef].Directives.Refs {
		if v.Operation.Input.ByteSliceString(v.Operation.Directives[ref].Name) != formatDateTimeDirective {
			continue
		}

		arguments := make(map[string]string, 2)
		formatValue, ok := v.Operation.DirectiveArgumentValueByName(ref, []byte(formatDateTimeArgFormat))
		if ok && formatValue.Kind == ast.ValueKindEnum {
			arguments[formatDateTimeArgFormat] = v.Operation.EnumValueNameString(formatValue.Ref)
		}
		customFormatValue, ok := v.Operation.DirectiveArgumentValueByName(ref, []byte(formatDateTimeArgCustomFormat))
		if ok && customFormatValue.Kind == ast.ValueKindString {
			arguments[formatDateTimeArgCustomFormat] = v.Operation.StringValueContentString(customFormatValue.Ref)
		}
		return arguments
	}
	return nil
}
