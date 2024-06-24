package resolve

import (
	"context"
	"errors"
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/datasource/httpclient"
	"github.com/wundergraph/graphql-go-tools/pkg/fastbuffer"
	"github.com/wundergraph/graphql-go-tools/pkg/lexer/literal"
)

type SegmentType int

const (
	StaticSegmentType SegmentType = iota + 1
	VariableSegmentType
)

type TemplateSegment struct {
	SegmentType        SegmentType
	Data               []byte
	VariableKind       VariableKind
	VariableSourcePath []string
	VariableGenerated  bool
	VariableNullable   bool
	Renderer           VariableRenderer
}

type InputTemplate struct {
	Segments []TemplateSegment
	// SetTemplateOutputToNullOnVariableNull will safely return "null" if one of the template variables renders to null
	// This is the case, e.g. when using batching and one sibling is null, resulting in a null value for one batch item
	// Returning null in this case tells the batch implementation to skip this item
	SetTemplateOutputToNullOnVariableNull bool
	ResetInputTemplateFunc                func(*Context, map[string]bool) InputTemplate
	RewriteVariableFunc                   func(*Context, []byte, jsonparser.ValueType) ([]byte, error)
}

const unrenderVariableKeyFormat = "unrender_variables(%s)"

type UnrenderVariable struct {
	Name       string
	Nullable   bool
	Generated  bool
	ValueIndex int
	ValueType  jsonparser.ValueType
	Renderer   VariableRenderer
}

func GetUnrenderVariables(ctx context.Context, preparedInputBytes []byte) ([]UnrenderVariable, bool) {
	unrenderVariables, ok := ctx.Value(fmt.Sprintf(unrenderVariableKeyFormat, string(preparedInputBytes))).([]UnrenderVariable)
	return unrenderVariables, ok
}

var setTemplateOutputNull = errors.New("set to null")

func (i *InputTemplate) AddTemplateSegment(segment TemplateSegment) {
	i.Segments = append(i.Segments, segment)
}

func (i *InputTemplate) AddStaticTemplateSegment(data string) {
	i.AddTemplateSegment(TemplateSegment{SegmentType: StaticSegmentType, Data: []byte(data)})
}

func (i *InputTemplate) Render(ctx *Context, data []byte, preparedInput *fastbuffer.FastBuffer) (err error) {
	undefinedVariables, unrenderVariables := make([]string, 0), make([]UnrenderVariable, 0)

	for _, segment := range i.Segments {
		switch segment.SegmentType {
		case StaticSegmentType:
			preparedInput.WriteBytes(segment.Data)
		case VariableSegmentType:
			switch segment.VariableKind {
			case ObjectVariableKind:
				err = i.renderObjectVariable(ctx, data, segment, preparedInput)
			case ContextVariableKind:
				err = i.renderContextVariable(ctx, segment, preparedInput, &undefinedVariables, &unrenderVariables)
			case HeaderVariableKind:
				err = i.renderHeaderVariable(ctx, segment.VariableSourcePath, preparedInput)
			default:
				err = fmt.Errorf("InputTemplate.Render: cannot resolve variable of kind: %d", segment.VariableKind)
			}
			if err != nil {
				if errors.Is(err, setTemplateOutputNull) {
					preparedInput.Reset()
					preparedInput.WriteBytes(literal.NULL)
					return nil
				}
				return err
			}
		}
	}

	if len(undefinedVariables) > 0 {
		ctx.Context = httpclient.CtxSetUndefinedVariables(ctx.Context, undefinedVariables)
	}
	if len(unrenderVariables) > 0 {
		ctx.Context = context.WithValue(ctx.Context, fmt.Sprintf(unrenderVariableKeyFormat, preparedInput.String()), unrenderVariables)
	}

	return
}

func (i *InputTemplate) renderObjectVariable(ctx context.Context, variables []byte, segment TemplateSegment, preparedInput *fastbuffer.FastBuffer) error {
	value, valueType, offset, err := jsonparser.Get(variables, segment.VariableSourcePath...)
	if err != nil || valueType == jsonparser.Null {
		if i.SetTemplateOutputToNullOnVariableNull {
			return setTemplateOutputNull
		}
		preparedInput.WriteBytes(literal.NULL)
		return nil
	}
	if valueType == jsonparser.String {
		value = variables[offset-len(value)-2 : offset]
		switch segment.Renderer.GetKind() {
		case VariableRendererKindPlain, VariableRendererKindPlanWithValidation:
			if plainRenderer, ok := (segment.Renderer).(*PlainVariableRenderer); ok {
				plainRenderer.rootValueType.Value = valueType
			}
		}
	}
	return segment.Renderer.RenderVariable(ctx, value, preparedInput)
}

func (i *InputTemplate) renderContextVariable(ctx *Context, segment TemplateSegment, preparedInput *fastbuffer.FastBuffer,
	undefinedVariables *[]string, unrenderVariables *[]UnrenderVariable) error {
	value, valueType, offset, err := jsonparser.Get(ctx.Variables, segment.VariableSourcePath...)
	if err == nil && i.RewriteVariableFunc != nil {
		value, err = i.RewriteVariableFunc(ctx, value, valueType)
	}
	if err != nil || valueType == jsonparser.Null {
		*unrenderVariables = append(*unrenderVariables, UnrenderVariable{
			Name:       segment.VariableSourcePath[0],
			Nullable:   segment.VariableNullable,
			Generated:  segment.VariableGenerated,
			ValueIndex: preparedInput.Len(),
			ValueType:  valueType,
			Renderer:   segment.Renderer,
		})
		if errors.Is(err, jsonparser.KeyPathNotFoundError) {
			*undefinedVariables = append(*undefinedVariables, segment.VariableSourcePath[0])
		}
		preparedInput.WriteBytes(literal.NULL)
		return nil
	}
	if valueType == jsonparser.String {
		if i.RewriteVariableFunc != nil {
			value = []byte(`"` + string(value) + `"`)
		} else {
			value = ctx.Variables[offset-len(value)-2 : offset]
		}
		switch segment.Renderer.GetKind() {
		case VariableRendererKindPlain, VariableRendererKindPlanWithValidation:
			if plainRenderer, ok := (segment.Renderer).(*PlainVariableRenderer); ok {
				plainRenderer.rootValueType.Value = valueType
			}
		}
	}
	return segment.Renderer.RenderVariable(ctx, value, preparedInput)
}

func (i *InputTemplate) renderHeaderVariable(ctx *Context, path []string, preparedInput *fastbuffer.FastBuffer) error {
	if len(path) != 1 {
		return errHeaderPathInvalid
	}
	value := ctx.Request.Header.Values(path[0])
	if len(value) == 0 {
		return nil
	}
	if len(value) == 1 {
		preparedInput.WriteString(value[0])
		return nil
	}
	for j := range value {
		if j != 0 {
			preparedInput.WriteBytes(literal.COMMA)
		}
		preparedInput.WriteString(value[j])
	}
	return nil
}
