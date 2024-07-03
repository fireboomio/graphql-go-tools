package plan

import (
	"github.com/wundergraph/graphql-go-tools/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/resolve"
	"golang.org/x/exp/slices"
)

func (v *Visitor) setWaitExportedRequiredForArguments(config objectFetchConfiguration) {
	objectPopField := config.objectPopField
	if objectPopField == nil || objectPopField.WaitExportedRequired || objectPopField.LengthOfExportedBefore == 0 {
		return
	}
	for _, item := range config.object.Fields {
		if item.WaitExportedRequired {
			objectPopField.WaitExportedRequired = true
			return
		}
	}

	var variables []resolve.Variable
	switch f := config.object.Fetch.(type) {
	case *resolve.SingleFetch:
		variables = f.Variables
	case *resolve.BatchFetch:
		variables = f.Fetch.Variables
	default:
		return
	}
	for _, item := range variables {
		if item.GetVariableKind() != resolve.ContextVariableKind {
			continue
		}
		segment := item.TemplateSegment()
		if segment.VariableGenerated {
			continue
		}
		for variable, index := range v.exportedVariables {
			if variable == segment.VariableSourcePath[0] && index < objectPopField.LengthOfExportedBefore {
				objectPopField.WaitExportedRequired = true
				return
			}
		}
	}
}

func (v *Visitor) resetWaitExportedRequired(ref int) {
	index, ok := v.currentFieldIndexes[ref]
	if !ok || index >= len(v.currentFieldIndexes) {
		return
	}

	currentField := v.currentFields[index].popField
	if currentField.LengthOfExportedBefore == 0 || currentField.WaitExportedRequired {
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
