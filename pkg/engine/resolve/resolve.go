//go:generate mockgen --build_flags=--mod=mod -self_package=github.com/wundergraph/graphql-go-tools/pkg/engine/resolve -destination=resolve_mock_test.go -package=resolve . DataSource,BeforeFetchHook,AfterFetchHook,DataSourceBatch,DataSourceBatchFactory

package resolve

import (
	"bytes"
	"context"
	"fmt"
	"golang.org/x/exp/maps"
	"io"
	"net/http"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/cespare/xxhash/v2"
	"github.com/tidwall/gjson"
	errors "golang.org/x/xerrors"

	"github.com/wundergraph/graphql-go-tools/internal/pkg/unsafebytes"
	"github.com/wundergraph/graphql-go-tools/pkg/fastbuffer"
	"github.com/wundergraph/graphql-go-tools/pkg/lexer/literal"
	"github.com/wundergraph/graphql-go-tools/pkg/pool"
)

var (
	lBrace            = []byte("{")
	rBrace            = []byte("}")
	lBrack            = []byte("[")
	rBrack            = []byte("]")
	comma             = []byte(",")
	colon             = []byte(":")
	quote             = []byte("\"")
	quotedComma       = []byte(`","`)
	null              = []byte("null")
	literalData       = []byte("data")
	literalErrors     = []byte("errors")
	literalMessage    = []byte("message")
	literalLocations  = []byte("locations")
	literalLine       = []byte("line")
	literalColumn     = []byte("column")
	literalPath       = []byte("path")
	literalExtensions = []byte("extensions")

	unableToResolveMsg       = []byte("unable to resolve")
	unableToResolveMsgFormat = "unable to resolve with data (%s) on path (%s)[%s:%d]"
	emptyArray               = []byte("[]")
)

var (
	errNonNullableFieldValueIsNull = errors.New("non Nullable field value is null")
	errTypeNameSkipped             = errors.New("skipped because of __typename condition")
	errHeaderPathInvalid           = errors.New("invalid header path: header variables must be of this format: .request.header.{{ key }} ")

	ErrUnableToResolve = errors.New("unable to resolve operation")
)

var (
	responsePaths = [][]string{
		{"errors"},
		{"data"},
	}
	errorPaths = [][]string{
		{"message"},
		{"locations"},
		{"path"},
		{"extensions"},
	}
	entitiesPath = []string{"_entities"}
)

const (
	rootErrorsPathIndex = 0
	rootDataPathIndex   = 1

	errorsMessagePathIndex    = 0
	errorsLocationsPathIndex  = 1
	errorsPathPathIndex       = 2
	errorsExtensionsPathIndex = 3
)

type Node interface {
	NodeKind() NodeKind
}

type NodeSkip interface {
	NodePath() []string
	NodeZeroValue(bool) []byte
}

type NodeZeroValue struct {
	Path      []string
	JsonPath  []string
	ZeroValue []byte
}

type NodeKind int
type FetchKind int

const (
	NodeKindObject NodeKind = iota + 1
	NodeKindEmptyObject
	NodeKindArray
	NodeKindEmptyArray
	NodeKindNull
	NodeKindString
	NodeKindBoolean
	NodeKindInteger
	NodeKindFloat

	FetchKindSingle FetchKind = iota + 1
	FetchKindParallel
	FetchKindBatch
)

type HookContext struct {
	CurrentPath []byte
}

type BeforeFetchHook interface {
	OnBeforeFetch(ctx HookContext, input []byte)
}

type AfterFetchHook interface {
	OnData(ctx HookContext, output []byte, singleFlight bool)
	OnError(ctx HookContext, output []byte, singleFlight bool)
}

type Context struct {
	context.Context
	RuleEvaluate        func([]byte, string) bool
	DateFormatFunc      func(map[string]string, string) string
	Variables           []byte
	Request             Request
	pathElements        [][]byte
	responseElements    []string
	lastFetchID         int
	patches             []patch
	usedBuffers         []*bytes.Buffer
	currentPatch        int
	maxPatch            int
	pathPrefix          []byte
	dataLoader          *dataLoader
	beforeFetchHook     BeforeFetchHook
	afterFetchHook      AfterFetchHook
	position            Position
	RenameTypeNames     []RenameTypeName
	skipFieldZeroValues map[*Field]*NodeZeroValue
}

type Request struct {
	Header http.Header
}

func NewContext(ctx context.Context) *Context {
	return &Context{
		Context:      ctx,
		Variables:    make([]byte, 0, 4096),
		pathPrefix:   make([]byte, 0, 4096),
		pathElements: make([][]byte, 0, 16),
		patches:      make([]patch, 0, 48),
		usedBuffers:  make([]*bytes.Buffer, 0, 48),
		currentPatch: -1,
		maxPatch:     -1,
		position:     Position{},
		dataLoader:   nil,
	}
}

func (c *Context) Clone() Context {
	variables := make([]byte, len(c.Variables))
	copy(variables, c.Variables)
	pathPrefix := make([]byte, len(c.pathPrefix))
	copy(pathPrefix, c.pathPrefix)
	pathElements := make([][]byte, len(c.pathElements))
	for i := range pathElements {
		pathElements[i] = make([]byte, len(c.pathElements[i]))
		copy(pathElements[i], c.pathElements[i])
	}
	responseElements := make([]string, len(c.responseElements))
	copy(responseElements, c.responseElements)
	patches := make([]patch, len(c.patches))
	for i := range patches {
		patches[i] = patch{
			path:      make([]byte, len(c.patches[i].path)),
			extraPath: make([]byte, len(c.patches[i].extraPath)),
			data:      make([]byte, len(c.patches[i].data)),
			index:     c.patches[i].index,
		}
		copy(patches[i].path, c.patches[i].path)
		copy(patches[i].extraPath, c.patches[i].extraPath)
		copy(patches[i].data, c.patches[i].data)
	}
	return Context{
		Context:          c.Context,
		RuleEvaluate:     c.RuleEvaluate,
		Variables:        variables,
		Request:          c.Request,
		pathElements:     pathElements,
		responseElements: responseElements,
		lastFetchID:      c.lastFetchID,
		patches:          patches,
		usedBuffers:      make([]*bytes.Buffer, 0, 48),
		currentPatch:     c.currentPatch,
		maxPatch:         c.maxPatch,
		pathPrefix:       pathPrefix,
		dataLoader:       c.dataLoader,
		beforeFetchHook:  c.beforeFetchHook,
		afterFetchHook:   c.afterFetchHook,
		position:         c.position,
	}
}

func (c *Context) Free() {
	c.Context = nil
	c.RuleEvaluate = nil
	c.Variables = c.Variables[:0]
	c.Request.Header = nil
	c.pathElements = c.pathElements[:0]
	c.responseElements = c.responseElements[:0]
	c.lastFetchID = 0
	c.patches = c.patches[:0]
	for i := range c.usedBuffers {
		pool.BytesBuffer.Put(c.usedBuffers[i])
	}
	c.usedBuffers = c.usedBuffers[:0]
	c.currentPatch = -1
	c.maxPatch = -1
	c.pathPrefix = c.pathPrefix[:0]
	c.dataLoader = nil
	c.beforeFetchHook = nil
	c.afterFetchHook = nil
	c.position = Position{}
	c.RenameTypeNames = nil
	c.skipFieldZeroValues = nil
}

func (c *Context) SetBeforeFetchHook(hook BeforeFetchHook) {
	c.beforeFetchHook = hook
}

func (c *Context) SetAfterFetchHook(hook AfterFetchHook) {
	c.afterFetchHook = hook
}

func (c *Context) setPosition(position Position) {
	c.position = position
}

func (c *Context) addResponseElements(elements []string) {
	c.responseElements = append(c.responseElements, elements...)
}

func (c *Context) addResponseArrayElements(elements []string) {
	c.responseElements = append(c.responseElements, elements...)
	c.responseElements = append(c.responseElements, arrayElementKey)
}

func (c *Context) removeResponseLastElements(elements []string) {
	c.responseElements = c.responseElements[:len(c.responseElements)-len(elements)]
}
func (c *Context) removeResponseArrayLastElements(elements []string) {
	c.responseElements = c.responseElements[:len(c.responseElements)-(len(elements)+1)]
}

func (c *Context) resetResponsePathElements() {
	c.responseElements = nil
}

func (c *Context) addPathElement(elem []byte) {
	c.pathElements = append(c.pathElements, elem)
}

func (c *Context) addIntegerPathElement(elem int) {
	b := unsafebytes.StringToBytes(strconv.Itoa(elem))
	c.pathElements = append(c.pathElements, b)
}

func (c *Context) removeLastPathElement() {
	c.pathElements = c.pathElements[:len(c.pathElements)-1]
}

func (c *Context) path() []byte {
	buf := pool.BytesBuffer.Get()
	c.usedBuffers = append(c.usedBuffers, buf)
	if len(c.pathPrefix) != 0 {
		buf.Write(c.pathPrefix)
	} else {
		buf.Write(literal.SLASH)
		buf.Write(literal.DATA)
	}
	for i := range c.pathElements {
		if i == 0 && bytes.Equal(literal.DATA, c.pathElements[0]) {
			continue
		}
		_, _ = buf.Write(literal.SLASH)
		_, _ = buf.Write(c.pathElements[i])
	}
	return buf.Bytes()
}

func (c *Context) addPatch(index int, path, extraPath, data []byte) {
	next := patch{path: path, extraPath: extraPath, data: data, index: index}
	c.patches = append(c.patches, next)
	c.maxPatch++
}

func (c *Context) popNextPatch() (patch patch, ok bool) {
	c.currentPatch++
	if c.currentPatch > c.maxPatch {
		return patch, false
	}
	return c.patches[c.currentPatch], true
}

type patch struct {
	path, extraPath, data []byte
	index                 int
}

type Fetch interface {
	FetchKind() FetchKind
}

type Fetches []Fetch

type DataSourceBatchFactory interface {
	CreateBatch(inputs [][]byte) (DataSourceBatch, error)
}

type DataSourceBatch interface {
	Demultiplex(responseBufPair *BufPair, outputBuffers []*BufPair) (err error)
	Input() *fastbuffer.FastBuffer
}

type DataSource interface {
	Load(ctx context.Context, input []byte, w io.Writer) (err error)
}

type SubscriptionDataSource interface {
	Start(ctx context.Context, input []byte, next chan<- []byte) error
}

type Resolver struct {
	ctx               context.Context
	dataLoaderEnabled bool
	resultSetPool     sync.Pool
	byteSlicesPool    sync.Pool
	waitGroupPool     sync.Pool
	bufPairPool       sync.Pool
	bufPairSlicePool  sync.Pool
	errChanPool       sync.Pool
	hash64Pool        sync.Pool
	dataloaderFactory *dataLoaderFactory
	fetcher           *Fetcher
}

type inflightFetch struct {
	waitLoad sync.WaitGroup
	waitFree sync.WaitGroup
	err      error
	bufPair  BufPair
}

// New returns a new Resolver, ctx.Done() is used to cancel all active subscriptions & streams
func New(ctx context.Context, fetcher *Fetcher, enableDataLoader bool) *Resolver {
	return &Resolver{
		ctx: ctx,
		resultSetPool: sync.Pool{
			New: func() interface{} {
				return &resultSet{
					buffers: make(map[int]*BufPair, 8),
				}
			},
		},
		byteSlicesPool: sync.Pool{
			New: func() interface{} {
				slice := make([][]byte, 0, 24)
				return &slice
			},
		},
		waitGroupPool: sync.Pool{
			New: func() interface{} {
				return &sync.WaitGroup{}
			},
		},
		bufPairPool: sync.Pool{
			New: func() interface{} {
				pair := BufPair{
					Data:   fastbuffer.New(),
					Errors: fastbuffer.New(),
				}
				return &pair
			},
		},
		bufPairSlicePool: sync.Pool{
			New: func() interface{} {
				slice := make([]*BufPair, 0, 24)
				return &slice
			},
		},
		errChanPool: sync.Pool{
			New: func() interface{} {
				return make(chan error, 1)
			},
		},
		hash64Pool: sync.Pool{
			New: func() interface{} {
				return xxhash.New()
			},
		},
		dataloaderFactory: newDataloaderFactory(fetcher),
		fetcher:           fetcher,
		dataLoaderEnabled: enableDataLoader,
	}
}

func (r *Resolver) resolveNode(ctx *Context, node Node, data []byte, bufPair *BufPair) (err error) {
	switch n := node.(type) {
	case *Object:
		return r.resolveObject(ctx, n, data, bufPair)
	case *Array:
		return r.resolveArray(ctx, n, data, bufPair)
	case *Null:
		if n.Defer.Enabled {
			r.preparePatch(ctx, n.Defer.PatchIndex, nil, data)
		}
		r.resolveNull(bufPair.Data)
		return
	case *String:
		return r.resolveString(ctx, n, data, bufPair)
	case *Boolean:
		return r.resolveBoolean(ctx, n, data, bufPair)
	case *Integer:
		return r.resolveInteger(ctx, n, data, bufPair)
	case *Float:
		return r.resolveFloat(ctx, n, data, bufPair)
	case *EmptyObject:
		r.resolveEmptyObject(bufPair.Data)
		return
	case *EmptyArray:
		r.resolveEmptyArray(bufPair.Data)
		return
	default:
		return
	}
}

func (r *Resolver) validateContext(ctx *Context) (err error) {
	if ctx.maxPatch != -1 || ctx.currentPatch != -1 {
		return fmt.Errorf("Context must be resetted using Free() before re-using it")
	}
	return nil
}

func extractResponse(responseData []byte, bufPair *BufPair, cfg ProcessResponseConfig) {
	if len(responseData) == 0 {
		return
	}

	if !cfg.ExtractGraphqlResponse {
		bufPair.Data.WriteBytes(responseData)
		return
	}

	jsonparser.EachKey(responseData, func(i int, bytes []byte, valueType jsonparser.ValueType, err error) {
		switch i {
		case rootErrorsPathIndex:
			_, _ = jsonparser.ArrayEach(bytes, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
				var (
					message, locations, path, extensions []byte
				)
				jsonparser.EachKey(value, func(i int, bytes []byte, valueType jsonparser.ValueType, err error) {
					switch i {
					case errorsMessagePathIndex:
						message = bytes
					case errorsLocationsPathIndex:
						locations = bytes
					case errorsPathPathIndex:
						path = bytes
					case errorsExtensionsPathIndex:
						extensions = bytes
					}
				}, errorPaths...)
				if message != nil {
					bufPair.WriteErr(message, locations, path, extensions)
				}
			})
		case rootDataPathIndex:
			if cfg.ExtractFederationEntities {
				data, _, _, _ := jsonparser.Get(bytes, entitiesPath...)
				bufPair.Data.WriteBytes(data)
				return
			}
			bufPair.Data.WriteBytes(bytes)
		}
	}, responsePaths...)
}

func (r *Resolver) ResolveGraphQLResponse(ctx *Context, response *GraphQLResponse, data []byte, writer io.Writer) (err error) {

	buf := r.getBufPair()
	defer r.freeBufPair(buf)

	responseBuf := r.getBufPair()
	defer r.freeBufPair(responseBuf)

	extractResponse(data, responseBuf, ProcessResponseConfig{ExtractGraphqlResponse: true})

	if data != nil {
		ctx.lastFetchID = initialValueID
	}

	if r.dataLoaderEnabled {
		ctx.dataLoader = r.dataloaderFactory.newDataLoader(responseBuf.Data.Bytes())
		defer func() {
			r.dataloaderFactory.freeDataLoader(ctx.dataLoader)
			ctx.dataLoader = nil
		}()
	}

	ignoreData := false
	err = r.resolveNode(ctx, response.Data, responseBuf.Data.Bytes(), buf)
	if err != nil {
		if !errors.Is(err, errNonNullableFieldValueIsNull) {
			return
		}
		ignoreData = true
	}
	if responseBuf.Errors.Len() > 0 {
		r.MergeBufPairErrors(responseBuf, buf)
	}

	return writeGraphqlResponse(buf, writer, ignoreData)
}

func writeAndFlush(writer FlushWriter, msg []byte) error {
	_, err := writer.Write(msg)
	if err != nil {
		return err
	}
	writer.Flush()
	return nil
}

func (r *Resolver) ResolveGraphQLSubscription(ctx *Context, subscription *GraphQLSubscription, writer FlushWriter) (err error) {

	buf := r.getBufPair()
	err = subscription.Trigger.InputTemplate.Render(ctx, nil, buf.Data)
	if err != nil {
		return
	}
	rendered := buf.Data.Bytes()
	subscriptionInput := make([]byte, len(rendered))
	copy(subscriptionInput, rendered)
	r.freeBufPair(buf)

	c, cancel := context.WithCancel(ctx)
	defer cancel()
	resolverDone := r.ctx.Done()

	next := make(chan []byte)
	if subscription.Trigger.Source == nil {
		msg := []byte(`{"errors":[{"message":"no data source found"}]}`)
		return writeAndFlush(writer, msg)
	}

	err = subscription.Trigger.Source.Start(c, subscriptionInput, next)
	if err != nil {
		if errors.Is(err, ErrUnableToResolve) {
			msg := []byte(`{"errors":[{"message":"unable to resolve"}]}`)
			return writeAndFlush(writer, msg)
		}
		return err
	}

	for {
		select {
		case <-resolverDone:
			return nil
		default:
			data, ok := <-next
			if !ok {
				return nil
			}
			err = r.ResolveGraphQLResponse(ctx, subscription.Response, data, writer)
			if err != nil {
				return err
			}
			writer.Flush()
		}
	}
}

func (r *Resolver) ResolveGraphQLStreamingResponse(ctx *Context, response *GraphQLStreamingResponse, data []byte, writer FlushWriter) (err error) {

	if err := r.validateContext(ctx); err != nil {
		return err
	}

	err = r.ResolveGraphQLResponse(ctx, response.InitialResponse, data, writer)
	if err != nil {
		return err
	}
	writer.Flush()

	nextFlush := time.Now().Add(time.Millisecond * time.Duration(response.FlushInterval))

	buf := pool.BytesBuffer.Get()
	defer pool.BytesBuffer.Put(buf)

	buf.Write(literal.LBRACK)

	done := ctx.Context.Done()

Loop:
	for {
		select {
		case <-done:
			return
		default:
			patch, ok := ctx.popNextPatch()
			if !ok {
				break Loop
			}

			if patch.index > len(response.Patches)-1 {
				continue
			}

			if buf.Len() != 1 {
				buf.Write(literal.COMMA)
			}

			preparedPatch := response.Patches[patch.index]
			err = r.ResolveGraphQLResponsePatch(ctx, preparedPatch, patch.data, patch.path, patch.extraPath, buf)
			if err != nil {
				return err
			}

			now := time.Now()
			if now.After(nextFlush) {
				buf.Write(literal.RBRACK)
				_, err = writer.Write(buf.Bytes())
				if err != nil {
					return err
				}
				writer.Flush()
				buf.Reset()
				buf.Write(literal.LBRACK)
				nextFlush = time.Now().Add(time.Millisecond * time.Duration(response.FlushInterval))
			}
		}
	}

	if buf.Len() != 1 {
		buf.Write(literal.RBRACK)
		_, err = writer.Write(buf.Bytes())
		if err != nil {
			return err
		}
		writer.Flush()
	}

	return
}

func (r *Resolver) ResolveGraphQLResponsePatch(ctx *Context, patch *GraphQLResponsePatch, data, path, extraPath []byte, writer io.Writer) (err error) {

	buf := r.getBufPair()
	defer r.freeBufPair(buf)

	ctx.pathPrefix = append(path, extraPath...)

	if patch.Fetch != nil {
		set := r.getResultSet()
		defer r.freeResultSet(set)
		err = r.resolveFetch(ctx, patch.Fetch, data, set)
		if err != nil {
			return err
		}
		_, ok := set.buffers[0]
		if ok {
			r.MergeBufPairErrors(set.buffers[0], buf)
			data = set.buffers[0].Data.Bytes()
		}
	}

	err = r.resolveNode(ctx, patch.Value, data, buf)
	if err != nil {
		return
	}

	hasErrors := buf.Errors.Len() != 0
	hasData := buf.Data.Len() != 0

	if hasErrors {
		return
	}

	if hasData {
		if hasErrors {
			err = writeSafe(err, writer, comma)
		}
		err = writeSafe(err, writer, lBrace)
		err = writeSafe(err, writer, quote)
		err = writeSafe(err, writer, literal.OP)
		err = writeSafe(err, writer, quote)
		err = writeSafe(err, writer, colon)
		err = writeSafe(err, writer, quote)
		err = writeSafe(err, writer, patch.Operation)
		err = writeSafe(err, writer, quote)
		err = writeSafe(err, writer, comma)
		err = writeSafe(err, writer, quote)
		err = writeSafe(err, writer, literal.PATH)
		err = writeSafe(err, writer, quote)
		err = writeSafe(err, writer, colon)
		err = writeSafe(err, writer, quote)
		err = writeSafe(err, writer, path)
		err = writeSafe(err, writer, quote)
		err = writeSafe(err, writer, comma)
		err = writeSafe(err, writer, quote)
		err = writeSafe(err, writer, literal.VALUE)
		err = writeSafe(err, writer, quote)
		err = writeSafe(err, writer, colon)
		_, err = writer.Write(buf.Data.Bytes())
		err = writeSafe(err, writer, rBrace)
	}

	return
}

func (r *Resolver) resolveEmptyArray(b *fastbuffer.FastBuffer) {
	b.WriteBytes(lBrack)
	b.WriteBytes(rBrack)
}

func (r *Resolver) resolveEmptyObject(b *fastbuffer.FastBuffer) {
	b.WriteBytes(lBrace)
	b.WriteBytes(rBrace)
}

func (r *Resolver) resolveArray(ctx *Context, array *Array, data []byte, arrayBuf *BufPair) (err error) {
	if len(array.Path) != 0 {
		data, _, _, _ = jsonparser.Get(data, array.Path...)
	}

	if bytes.Equal(data, emptyArray) {
		r.resolveEmptyArray(arrayBuf.Data)
		return
	}

	arrayItems := r.byteSlicesPool.Get().(*[][]byte)
	defer func() {
		*arrayItems = (*arrayItems)[:0]
		r.byteSlicesPool.Put(arrayItems)
	}()

	_, _ = jsonparser.ArrayEach(data, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
		if err == nil && dataType == jsonparser.String {
			value = data[offset-2 : offset+len(value)] // add quotes to string values
		}

		*arrayItems = append(*arrayItems, value)
	})

	if len(*arrayItems) == 0 {
		if !array.Nullable {
			r.resolveEmptyArray(arrayBuf.Data)
			return errNonNullableFieldValueIsNull
		}
		r.resolveNull(arrayBuf.Data)
		return nil
	}

	ctx.addResponseArrayElements(array.Path)
	defer func() { ctx.removeResponseArrayLastElements(array.Path) }()

	if array.ResolveAsynchronous && !array.Stream.Enabled && !r.dataLoaderEnabled {
		return r.resolveArrayAsynchronous(ctx, array, arrayItems, arrayBuf)
	}
	return r.resolveArraySynchronous(ctx, array, arrayItems, arrayBuf)
}

func (r *Resolver) resolveArraySynchronous(ctx *Context, array *Array, arrayItems *[][]byte, arrayBuf *BufPair) (err error) {

	itemBuf := r.getBufPair()
	defer r.freeBufPair(itemBuf)

	arrayBuf.Data.WriteBytes(lBrack)
	var (
		hasPreviousItem bool
		dataWritten     int
	)
	for i := range *arrayItems {

		if array.Stream.Enabled {
			if i > array.Stream.InitialBatchSize-1 {
				ctx.addIntegerPathElement(i)
				r.preparePatch(ctx, array.Stream.PatchIndex, nil, (*arrayItems)[i])
				ctx.removeLastPathElement()
				continue
			}
		}

		ctx.addIntegerPathElement(i)
		err = r.resolveNode(ctx, array.Item, (*arrayItems)[i], itemBuf)
		ctx.removeLastPathElement()
		if err != nil {
			if errors.Is(err, errNonNullableFieldValueIsNull) && array.Nullable {
				arrayBuf.Data.Reset()
				r.resolveNull(arrayBuf.Data)
				return nil
			}
			if errors.Is(err, errTypeNameSkipped) {
				err = nil
				continue
			}
			return
		}
		dataWritten += itemBuf.Data.Len()
		r.MergeBufPairs(itemBuf, arrayBuf, hasPreviousItem)
		if !hasPreviousItem && dataWritten != 0 {
			hasPreviousItem = true
		}
	}

	arrayBuf.Data.WriteBytes(rBrack)
	return
}

func (r *Resolver) resolveArrayAsynchronous(ctx *Context, array *Array, arrayItems *[][]byte, arrayBuf *BufPair) (err error) {

	arrayBuf.Data.WriteBytes(lBrack)

	bufSlice := r.getBufPairSlice()
	defer r.freeBufPairSlice(bufSlice)

	wg := r.getWaitGroup()
	defer r.freeWaitGroup(wg)

	errCh := r.getErrChan()
	defer r.freeErrChan(errCh)

	wg.Add(len(*arrayItems))

	for i := range *arrayItems {
		itemBuf := r.getBufPair()
		*bufSlice = append(*bufSlice, itemBuf)
		itemData := (*arrayItems)[i]
		cloned := ctx.Clone()
		go func(ctx Context, i int) {
			ctx.addPathElement([]byte(strconv.Itoa(i)))
			if e := r.resolveNode(&ctx, array.Item, itemData, itemBuf); e != nil && !errors.Is(e, errTypeNameSkipped) {
				select {
				case errCh <- e:
				default:
				}
			}
			ctx.Free()
			wg.Done()
		}(cloned, i)
	}

	wg.Wait()

	select {
	case err = <-errCh:
	default:
	}

	if err != nil {
		if errors.Is(err, errNonNullableFieldValueIsNull) && array.Nullable {
			arrayBuf.Data.Reset()
			r.resolveNull(arrayBuf.Data)
			return nil
		}
		return
	}

	var (
		hasPreviousItem bool
		dataWritten     int
	)
	for i := range *bufSlice {
		dataWritten += (*bufSlice)[i].Data.Len()
		r.MergeBufPairs((*bufSlice)[i], arrayBuf, hasPreviousItem)
		if !hasPreviousItem && dataWritten != 0 {
			hasPreviousItem = true
		}
	}

	arrayBuf.Data.WriteBytes(rBrack)
	return
}

func (r *Resolver) exportField(ctx *Context, export *FieldExport, value []byte, zeroValue ...[]byte) {
	if export == nil {
		return
	}
	if export.AsString {
		value = append(literal.QUOTE, append(value, literal.QUOTE...)...)
	}
	if export.AsArray {
		dataValue, dataType, _, _ := jsonparser.Get(ctx.Variables, export.Path...)
		switch dataType {
		case jsonparser.Array:
			var expectedIndex int
			_, _ = jsonparser.ArrayEach(dataValue, func([]byte, jsonparser.ValueType, int, error) {
				expectedIndex++
			})
			copyValue := make([]byte, len(dataValue)-1)
			copy(copyValue, dataValue)
			if expectedIndex > 0 {
				copyValue = append(copyValue, literal.COMMA...)
			}
			dataValue = copyValue
		case jsonparser.NotExist, jsonparser.Null:
			dataValue = []byte("[")
		default:
			return
		}

		dataValue = append(dataValue, value...)
		value = append(dataValue, literal.RBRACK...)
	} else if export.AsBoolean && len(zeroValue) > 0 {
		value = literal.TRUE
		for _, item := range zeroValue {
			if bytes.Equal(item, value) {
				value = literal.FALSE
				break
			}
		}
	}
	ctx.Variables, _ = jsonparser.Set(ctx.Variables, value, export.Path...)
}

func (r *Resolver) resolveInteger(ctx *Context, integer *Integer, data []byte, integerBuf *BufPair) error {
	value, dataType, _, err := jsonparser.Get(data, integer.Path...)
	if err != nil || dataType != jsonparser.Number {
		if !integer.Nullable {
			return errNonNullableFieldValueIsNull
		}
		r.resolveNull(integerBuf.Data)
		return nil
	}
	integerBuf.Data.WriteBytes(value)
	r.exportField(ctx, integer.Export, value, literal.ZeroNumberValue)
	return nil
}

func (r *Resolver) resolveFloat(ctx *Context, floatValue *Float, data []byte, floatBuf *BufPair) error {
	value, dataType, _, err := jsonparser.Get(data, floatValue.Path...)
	if err != nil || dataType != jsonparser.Number {
		if !floatValue.Nullable {
			return errNonNullableFieldValueIsNull
		}
		r.resolveNull(floatBuf.Data)
		return nil
	}
	floatBuf.Data.WriteBytes(value)
	r.exportField(ctx, floatValue.Export, value, literal.ZeroNumberValue)
	return nil
}

func (r *Resolver) resolveBoolean(ctx *Context, boolean *Boolean, data []byte, booleanBuf *BufPair) error {
	value, valueType, _, err := jsonparser.Get(data, boolean.Path...)
	if err != nil || valueType != jsonparser.Boolean {
		if !boolean.Nullable {
			return errNonNullableFieldValueIsNull
		}
		r.resolveNull(booleanBuf.Data)
		return nil
	}
	booleanBuf.Data.WriteBytes(value)
	r.exportField(ctx, boolean.Export, value)
	return nil
}

const (
	prismaTypeKey  = "prisma__type"
	prismaValueKey = "prisma__value"
	queryRawKey    = "queryRaw"
)

type (
	QueryRawType struct {
		FieldIndex int
		ValueTypes map[string]string
	}
	QueryRawResp struct{}
)

func handleQueryRawResp(ctx *Context, value, data []byte) (result []byte) {
	if data, _, _, _ = jsonparser.Get(data, queryRawKey); data == nil {
		return
	}

	var (
		rawValueTypes         map[string]string
		rawValueTypesRequired bool
	)
	if rawTypes, typesFound := ctx.Value(QueryRawResp{}).(map[string]*QueryRawType); typesFound {
		if rawType, typeFound := rawTypes[getRawFieldPath(ctx)]; typeFound {
			rawType.ValueTypes = make(map[string]string)
			rawValueTypes, rawValueTypesRequired = rawType.ValueTypes, true
		}
	}
	itemObjectIndex, result := -1, value
	_, _ = jsonparser.ArrayEach(value, func(itemObject []byte, _ jsonparser.ValueType, _ int, _ error) {
		itemObjectIndex++
		_ = jsonparser.ObjectEach(itemObject, func(key []byte, itemRaw []byte, _ jsonparser.ValueType, _ int) error {
			itemKey := string(key)
			itemObject, _ = jsonparser.Set(itemObject, getValueBytes(itemRaw, prismaValueKey, true), itemKey)
			if rawValueTypesRequired {
				if prismaType, ok := rawValueTypes[itemKey]; !ok || prismaType == "null" {
					rawValueTypes[itemKey] = string(getValueBytes(itemRaw, prismaTypeKey, false))
				}
			}
			return nil
		})
		result, _ = jsonparser.Set(result, itemObject, fmt.Sprintf("[%d]", itemObjectIndex))
	})
	return
}

func getRawFieldPath(ctx *Context) string {
	buf := pool.BytesBuffer.Get()
	defer pool.BytesBuffer.Put(buf)
	for _, item := range ctx.pathElements {
		buf.WriteString(string(item))
		buf.Write(literal.DOT)
	}
	return buf.String()[:buf.Len()-1]
}

func getValueBytes(data []byte, key string, matchValueType bool) []byte {
	value, valueType, offset, _ := jsonparser.Get(data, key)
	if matchValueType && valueType == jsonparser.String {
		value = data[offset-len(value)-2 : offset]
	}
	return value
}

const executeRawKey = "executeRaw"

type (
	ExecuteRawType struct {
		FieldIndex int
		ValueType  jsonparser.ValueType
	}
	ExecuteRawResp struct{}
)

func handleExecuteRawResp(ctx *Context, value, data []byte) []byte {
	data, dataType, _, _ := jsonparser.Get(data, executeRawKey)
	if data == nil {
		return nil
	}
	if rawTypes, typesFound := ctx.Value(ExecuteRawResp{}).(map[string]*ExecuteRawType); typesFound {
		if rawType, typeFound := rawTypes[getRawFieldPath(ctx)]; typeFound {
			rawType.ValueType = dataType
		}
	}
	return value
}

func (r *Resolver) resolveString(ctx *Context, str *String, data []byte, stringBuf *BufPair) error {
	var (
		value     []byte
		valueType jsonparser.ValueType
		err       error
	)

	value, valueType, _, err = jsonparser.Get(data, str.Path...)
	if err != nil || valueType != jsonparser.String {
		if err == nil && str.UnescapeResponseJson {
			switch valueType {
			case jsonparser.Object, jsonparser.Array, jsonparser.Boolean, jsonparser.Number, jsonparser.Null:
				stringBuf.Data.WriteBytes(value)
				return nil
			}
		}
		if value != nil && valueType != jsonparser.Null {
			if raw := handleQueryRawResp(ctx, value, data); raw != nil {
				stringBuf.Data.WriteBytes(raw)
				return nil
			}

			if raw := handleExecuteRawResp(ctx, value, data); raw != nil {
				stringBuf.Data.WriteBytes(raw)
				return nil
			}
			return fmt.Errorf("invalid value type '%s' for path %s, expecting string, got: %v. You can fix this by configuring this field as Int/Float/JSON Scalar", valueType, string(ctx.path()), string(value))
		}
		if !str.Nullable {
			return errNonNullableFieldValueIsNull
		}
		r.resolveNull(stringBuf.Data)
		return nil
	}

	if value == nil && !str.Nullable {
		return errNonNullableFieldValueIsNull
	}

	if str.UnescapeResponseJson {
		value = bytes.ReplaceAll(value, []byte(`\"`), []byte(`"`))

		// Do not modify values which was strings
		// When the original value from upstream response was a plain string value `"hello"`, `"true"`, `"1"`, `"2.0"`,
		// after getting it via jsonparser.Get we will get unquoted values `hello`, `true`, `1`, `2.0`
		// which is not string anymore, so we need to quote it again
		if !(bytes.ContainsAny(value, `{}[]`) && gjson.ValidBytes(value)) {
			// wrap value in quotes to make it valid json
			value = append(literal.QUOTE, append(value, literal.QUOTE...)...)
		}

		stringBuf.Data.WriteBytes(value)
		r.exportField(ctx, str.Export, value, literal.ZeroArrayValue, literal.ZeroObjectValue, literal.ZeroStringWithQuoteValue)
		return nil
	}

	value = r.renameTypeName(ctx, str, value)
	value = r.formatDateTime(ctx, str, value)
	stringBuf.Data.WriteBytes(quote)
	stringBuf.Data.WriteBytes(value)
	stringBuf.Data.WriteBytes(quote)
	r.exportField(ctx, str.Export, value, literal.ZeroStringValue)
	return nil
}

func (r *Resolver) renameTypeName(ctx *Context, str *String, typeName []byte) []byte {
	if !str.IsTypeName {
		return typeName
	}
	for i := range ctx.RenameTypeNames {
		if bytes.Equal(ctx.RenameTypeNames[i].From, typeName) {
			return ctx.RenameTypeNames[i].To
		}
	}
	return typeName
}

func (r *Resolver) preparePatch(ctx *Context, patchIndex int, extraPath, data []byte) {
	buf := pool.BytesBuffer.Get()
	ctx.usedBuffers = append(ctx.usedBuffers, buf)
	_, _ = buf.Write(data)
	path, data := ctx.path(), buf.Bytes()
	ctx.addPatch(patchIndex, path, extraPath, data)
}

func (r *Resolver) resolveNull(b *fastbuffer.FastBuffer) {
	b.WriteBytes(null)
}

func (r *Resolver) addResolveError(ctx *Context, objectBuf *BufPair, data []byte, path ...string) {
	locationsBuf, pathBuf := pool.BytesBuffer.Get(), pool.BytesBuffer.Get()
	defer pool.BytesBuffer.Put(locationsBuf)
	defer pool.BytesBuffer.Put(pathBuf)

	var pathBytes []byte

	locationsBuf.Write(lBrack)
	locationsBuf.Write(lBrace)
	locationsBuf.Write(quote)
	locationsBuf.Write(literalLine)
	locationsBuf.Write(quote)
	locationsBuf.Write(colon)
	locationsBuf.Write([]byte(strconv.Itoa(int(ctx.position.Line))))
	locationsBuf.Write(comma)
	locationsBuf.Write(quote)
	locationsBuf.Write(literalColumn)
	locationsBuf.Write(quote)
	locationsBuf.Write(colon)
	locationsBuf.Write([]byte(strconv.Itoa(int(ctx.position.Column))))
	locationsBuf.Write(rBrace)
	locationsBuf.Write(rBrack)

	if len(ctx.pathElements) > 0 {
		pathBuf.Write(lBrack)
		pathBuf.Write(quote)
		pathBuf.Write(bytes.Join(ctx.pathElements, quotedComma))
		pathBuf.Write(quote)
		pathBuf.Write(rBrack)

		pathBytes = pathBuf.Bytes()
	}

	messageBytes := unableToResolveMsg
	if _, file, line, ok := runtime.Caller(1); ok {
		messageBytes = []byte(fmt.Sprintf(unableToResolveMsgFormat, string(data), strings.Join(path, ","), filepath.Base(file), line))
	}
	objectBuf.WriteErr(messageBytes, locationsBuf.Bytes(), pathBytes, nil)
}

func (r *Resolver) resolveObject(ctx *Context, object *Object, data []byte, objectBuf *BufPair) (err error) {
	if len(object.Path) != 0 {
		data, _, _, _ = jsonparser.Get(data, object.Path...)

		if len(data) == 0 || bytes.Equal(data, literal.NULL) {
			// we will not traverse the children if the object is null
			// therefore, we must "pop" the null element from the batch
			r.recursivelySkipBatchResults(ctx, object, data)
			if object.Nullable {
				r.resolveNull(objectBuf.Data)
				return
			}

			r.addResolveError(ctx, objectBuf, data, object.Path...)
			return errNonNullableFieldValueIsNull
		}

		ctx.addResponseElements(object.Path)
		defer ctx.removeResponseLastElements(object.Path)
	}

	if object.UnescapeResponseJson {
		data = bytes.ReplaceAll(data, []byte(`\"`), []byte(`"`))
	}

	var set *resultSet
	if object.Fetch != nil {
		set = r.getResultSet()
		defer r.freeResultSet(set)
		r.setResultSetSkipData(ctx, object, set)
		if err = r.resolveFetch(ctx, object.Fetch, data, set); err != nil {
			return
		}
		for i := range set.buffers {
			r.MergeBufPairErrors(set.buffers[i], objectBuf)
		}
	}

	fieldBuf := r.getBufPair()
	defer r.freeBufPair(fieldBuf)

	responseElements := ctx.responseElements
	lastFetchID := ctx.lastFetchID

	typeNameSkip := false
	first := true
	skipCount := 0
	for i := range object.Fields {
		var (
			fieldData []byte
			fieldSkip bool
		)
		field := object.Fields[i]
		if set != nil && field.HasBuffer {
			if fieldData, fieldSkip, err = r.runDelayFuncOrSkip(ctx, field, data, objectBuf, set); err != nil {
				return
			}
		} else {
			fieldData, fieldSkip = r.setZeroValueOrSkip(ctx, field, data)
		}
		if fieldSkip {
			skipCount++
			continue
		}

		if field.OnTypeName != nil {
			typeName, _, _, _ := jsonparser.Get(fieldData, "__typename")
			if !bytes.Equal(typeName, field.OnTypeName) {
				typeNameSkip = true
				// Restore the response elements that may have been reset above.
				ctx.responseElements = responseElements
				ctx.lastFetchID = lastFetchID
				continue
			}
		}

		if first {
			objectBuf.Data.WriteBytes(lBrace)
			first = false
		} else {
			objectBuf.Data.WriteBytes(comma)
		}
		objectBuf.Data.WriteBytes(quote)
		objectBuf.Data.WriteBytes(field.Name)
		objectBuf.Data.WriteBytes(quote)
		objectBuf.Data.WriteBytes(colon)
		ctx.addPathElement(field.Name)
		ctx.setPosition(field.Position)
		if set != nil && field.HasBuffer {
			ctx.skipFieldZeroValues = set.skipBufferFieldZeroValues[field.BufferID]
		}
		err = r.resolveNode(ctx, field.Value, fieldData, fieldBuf)
		if set != nil && field.HasBuffer {
			maps.Clear(ctx.skipFieldZeroValues)
		}
		ctx.removeLastPathElement()
		ctx.responseElements = responseElements
		ctx.lastFetchID = lastFetchID
		if err != nil {
			if errors.Is(err, errTypeNameSkipped) {
				objectBuf.Data.Reset()
				r.resolveEmptyObject(objectBuf.Data)
				return nil
			}
			if errors.Is(err, errNonNullableFieldValueIsNull) {
				objectBuf.Data.Reset()
				r.MergeBufPairErrors(fieldBuf, objectBuf)

				if object.Nullable {
					r.resolveNull(objectBuf.Data)
					return nil
				}

				// if fied is of object type than we should not add resolve error here
				if _, ok := field.Value.(*Object); !ok {
					var fieldValuePath []string
					if nodeSkip, ok := field.Value.(NodeSkip); ok {
						fieldValuePath = nodeSkip.NodePath()
					}
					r.addResolveError(ctx, objectBuf, fieldData, fieldValuePath...)
				}
			}

			return
		}
		r.MergeBufPairs(fieldBuf, objectBuf, false)
	}
	allSkipped := len(object.Fields) != 0 && len(object.Fields) == skipCount
	if allSkipped {
		// return empty object if all fields have been skipped
		objectBuf.Data.WriteBytes(lBrace)
		objectBuf.Data.WriteBytes(rBrace)
		return
	}
	if first {
		if typeNameSkip && !object.Nullable {
			return errTypeNameSkipped
		}
		if !object.Nullable {
			r.addResolveError(ctx, objectBuf, data, object.Path...)
			return errNonNullableFieldValueIsNull
		}
		r.resolveNull(objectBuf.Data)
		return
	}
	objectBuf.Data.WriteBytes(rBrace)
	return
}

// recursivelySkipBatchResults traverses an object and skips all batch results by triggering fetch
// when a fetch is attached to an object using batch fetch, only the first object will actually trigger the fetch
// subsequent objects (siblings) will load the result from the cache, filled by the first sibling
// if one sibling has no data (null), we have to "pop" the null result (generated by the batch resolver) from the cache
// this is because the "null" sibling will not trigger a fetch by itself, as it has no data and will not resolve any fields
func (r *Resolver) recursivelySkipBatchResults(ctx *Context, object *Object, data []byte) {
	if object.Fetch != nil && object.Fetch.FetchKind() == FetchKindBatch {
		set := r.getResultSet()
		defer r.freeResultSet(set)
		_ = r.resolveFetch(ctx, object.Fetch, data, set)
	}
	for i := range object.Fields {
		value := object.Fields[i].Value
		switch v := value.(type) {
		case *Object:
			r.recursivelySkipBatchResults(ctx, v, data)
		case *Array:
			switch av := v.Item.(type) {
			case *Object:
				r.recursivelySkipBatchResults(ctx, av, data)
			}
		}
	}
}

func (r *Resolver) freeResultSet(set *resultSet) {
	for i := range set.buffers {
		set.buffers[i].Reset()
		r.bufPairPool.Put(set.buffers[i])
		delete(set.buffers, i)
	}
	for i := range set.skipBufferIds {
		delete(set.skipBufferIds, i)
	}
	for i := range set.delayFetchBufferFuncs {
		delete(set.delayFetchBufferFuncs, i)
	}
	for i := range set.skipBufferFieldZeroValues {
		delete(set.skipBufferFieldZeroValues, i)
	}
	r.resultSetPool.Put(set)
}

func (r *Resolver) resolveFetch(ctx *Context, fetch Fetch, data []byte, set *resultSet) (err error) {
	// if context is cancelled, we should not resolve the fetch
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil
	}

	switch f := fetch.(type) {
	case *SingleFetch:
		singleFetchFunc := r.buildSingleFetchFunc(f, set)
		if skip := r.skipOrSetDelayFunc(f, set, singleFetchFunc); skip {
			return
		}
		err = singleFetchFunc(ctx, data)
	case *BatchFetch:
		batchFetchFunc := r.buildBatchFetchFunc(f, set)
		if skip := r.skipOrSetDelayFunc(f.Fetch, set, batchFetchFunc); skip {
			return
		}
		err = batchFetchFunc(ctx, data)
	case *ParallelFetch:
		err = r.resolveParallelFetch(ctx, f, data, set)
	}
	return
}

func (r *Resolver) buildSingleFetchFunc(fetch *SingleFetch, set *resultSet) func(*Context, []byte) error {
	return func(ctx *Context, data []byte) error {
		set.buffers[fetch.BufferId] = r.getBufPair()
		return r.resolveSingleFetch(ctx, fetch, data, set)
	}
}

func (r *Resolver) buildBatchFetchFunc(fetch *BatchFetch, set *resultSet) func(*Context, []byte) error {
	return func(ctx *Context, data []byte) error {
		set.buffers[fetch.Fetch.BufferId] = r.getBufPair()
		return r.resolveBatchFetch(ctx, fetch, data, set)
	}
}

func (r *Resolver) resolveParallelFetch(ctx *Context, fetch *ParallelFetch, data []byte, set *resultSet) (err error) {
	preparedInputs := r.getBufPairSlice()
	defer r.freeBufPairSlice(preparedInputs)

	resolvers := make(map[*BufPair]func(*Context) error, len(fetch.Fetches))

	wg := r.getWaitGroup()
	defer r.freeWaitGroup(wg)

	var disallowParallelFetch bool
	for i := range fetch.Fetches {
		wg.Add(1)
		switch f := fetch.Fetches[i].(type) {
		case *SingleFetch:
			if skip := r.skipOrSetDelayFunc(f, set, r.buildSingleFetchFunc(f, set)); skip {
				wg.Done()
				continue
			}

			buf := r.getBufPair()
			set.buffers[f.BufferId] = buf
			resolvers[buf] = func(_ctx *Context) error { return r.resolveSingleFetch(_ctx, f, data, set) }
			disallowParallelFetch = disallowParallelFetch || f.DisallowParallelFetch
		case *BatchFetch:
			if skip := r.skipOrSetDelayFunc(f.Fetch, set, r.buildBatchFetchFunc(f, set)); skip {
				wg.Done()
				continue
			}

			buf := r.getBufPair()
			set.buffers[f.Fetch.BufferId] = buf
			resolvers[buf] = func(_ctx *Context) error { return r.resolveBatchFetch(_ctx, f, data, set) }
			disallowParallelFetch = disallowParallelFetch || f.Fetch.DisallowParallelFetch
		}
	}

	for bufPair, resolver := range resolvers {
		if disallowParallelFetch {
			if err = resolver(ctx); err != nil {
				return err
			}
			wg.Done()
		} else {
			clonedCtx := ctx.Clone()
			go func(c *Context, b *BufPair, r func(*Context) error) {
				if err = r(c); err != nil {
					b.WriteErr([]byte(err.Error()), nil, nil, nil)
				}
				clonedCtx.Free()
				wg.Done()
			}(&clonedCtx, bufPair, resolver)
		}
	}

	wg.Wait()

	return
}

func (r *Resolver) resolveBatchFetch(ctx *Context, fetch *BatchFetch, data []byte, set *resultSet) error {
	if r.dataLoaderEnabled {
		return ctx.dataLoader.LoadBatch(ctx, fetch, set)
	}

	preparedInput := r.getBufPair()
	defer r.freeBufPair(preparedInput)
	if err := set.renderInputTemplate(ctx, fetch.Fetch, data, preparedInput.Data); err != nil {
		return err
	}

	buf := set.buffers[fetch.Fetch.BufferId]
	return r.fetcher.FetchBatch(ctx, fetch, []*fastbuffer.FastBuffer{preparedInput.Data}, []*BufPair{buf})
}

func (r *Resolver) resolveSingleFetch(ctx *Context, fetch *SingleFetch, data []byte, set *resultSet) error {
	if r.dataLoaderEnabled && !fetch.DisableDataLoader {
		return ctx.dataLoader.Load(ctx, fetch, set)
	}

	preparedInput := r.getBufPair()
	defer r.freeBufPair(preparedInput)
	if err := set.renderInputTemplate(ctx, fetch, data, preparedInput.Data); err != nil {
		return err
	}

	buf := set.buffers[fetch.BufferId]
	return r.fetcher.Fetch(ctx, fetch, preparedInput.Data, buf)
}

type Object struct {
	Nullable             bool
	Path                 []string
	Fields               []*Field
	Fetch                Fetch
	UnescapeResponseJson bool `json:"unescape_response_json,omitempty"`
}

func (_ *Object) NodeKind() NodeKind {
	return NodeKindObject
}

func (e *Object) NodePath() []string {
	return e.Path
}

func (e *Object) NodeZeroValue(skipAll bool) []byte {
	if e.Nullable && !skipAll {
		return nil
	}
	return literal.ZeroObjectValue
}

func (e *Object) ExportedVariables() (variables []string) {
	for _, field := range e.Fields {
		if export, ok := field.Value.(FieldExportVariable); ok {
			variables = append(variables, export.ExportedVariables()...)
		}
	}
	return
}

type EmptyObject struct{}

func (_ *EmptyObject) NodeKind() NodeKind {
	return NodeKindEmptyObject
}

type EmptyArray struct{}

func (_ *EmptyArray) NodeKind() NodeKind {
	return NodeKindEmptyArray
}

type Field struct {
	Name                     []byte
	Value                    Node
	Position                 Position
	Defer                    *DeferField
	Stream                   *StreamField
	HasBuffer                bool
	BufferID                 int
	OnTypeName               []byte
	SkipDirective            SkipDirective
	IncludeDirective         IncludeDirective
	NoneExportedBefore       bool
	WaitExportedRequired     bool
	WaitExportedRequiredFunc func(*Context) bool

	// deprecated, only test on use
	SkipDirectiveDefined bool
	// deprecated, only test on use
	SkipVariableName string
	// deprecated, only test on use
	IncludeDirectiveDefined bool
	// deprecated, only test on use
	IncludeVariableName string
}

type (
	SkipDirective struct {
		Defined              bool
		VariableName         string
		Expression           string
		ExpressionIsVariable bool
	}
	IncludeDirective SkipDirective
)

type Position struct {
	Line   uint32
	Column uint32
}

type StreamField struct {
	InitialBatchSize int
}

type DeferField struct{}

type Null struct {
	Defer Defer
}

type Defer struct {
	Enabled    bool
	PatchIndex int
}

func (_ *Null) NodeKind() NodeKind {
	return NodeKindNull
}

type resultSet struct {
	buffers                   map[int]*BufPair
	skipBufferIds             map[int]bool
	delayFetchBufferFuncs     map[int]func(*Context, []byte) error
	skipBufferFieldZeroValues map[int]map[*Field]*NodeZeroValue
}

type SingleFetch struct {
	BufferId            int
	Input               string
	ResetInputFunc      func(*Context, map[string]bool) string
	RewriteVariableFunc func(*Context, []byte, jsonparser.ValueType) ([]byte, jsonparser.ValueType, error)
	DataSource          DataSource
	Variables           Variables
	// DisallowSingleFlight is used for write operations like mutations, POST, DELETE etc. to disable singleFlight
	// By default SingleFlight for fetches is disabled and needs to be enabled on the Resolver first
	// If the resolver allows SingleFlight it's up to each individual DataSource Planner to decide whether an Operation
	// should be allowed to use SingleFlight
	DisallowSingleFlight  bool
	DisallowParallelFetch bool
	DisableDataLoader     bool
	InputTemplate         InputTemplate
	DataSourceIdentifier  []byte
	ProcessResponseConfig ProcessResponseConfig
	// SetTemplateOutputToNullOnVariableNull will safely return "null" if one of the template variables renders to null
	// This is the case, e.g. when using batching and one sibling is null, resulting in a null value for one batch item
	// Returning null in this case tells the batch implementation to skip this item
	SetTemplateOutputToNullOnVariableNull bool
}

type ProcessResponseConfig struct {
	ExtractGraphqlResponse    bool
	ExtractFederationEntities bool
}

func (_ *SingleFetch) FetchKind() FetchKind {
	return FetchKindSingle
}

type ParallelFetch struct {
	Fetches []Fetch
}

func (_ *ParallelFetch) FetchKind() FetchKind {
	return FetchKindParallel
}

type BatchFetch struct {
	Fetch        *SingleFetch
	BatchFactory DataSourceBatchFactory
}

func (_ *BatchFetch) FetchKind() FetchKind {
	return FetchKindBatch
}

// FieldExport takes the value of the field during evaluation (rendering of the field)
// and stores it in the variables using the Path as JSON pointer.
type FieldExport struct {
	Path      []string
	AsArray   bool
	AsString  bool
	AsBoolean bool
}

type FieldExportVariable interface {
	ExportedVariables() []string
}

type String struct {
	Path                 []string
	Nullable             bool
	Export               *FieldExport      `json:"export,omitempty"`
	UnescapeResponseJson bool              `json:"unescape_response_json,omitempty"`
	IsTypeName           bool              `json:"is_type_name,omitempty"`
	DateFormatArguments  map[string]string `json:"-"`
}

func (_ *String) NodeKind() NodeKind {
	return NodeKindString
}

func (e *String) NodePath() []string {
	return e.Path
}

func (e *String) NodeZeroValue(bool) []byte {
	if e.Nullable {
		return nil
	}
	return literal.ZeroStringWithQuoteValue
}

func (e *String) ExportedVariables() (variables []string) {
	if e.Export != nil {
		variables = append(variables, e.Export.Path[0])
	}
	return
}

type Boolean struct {
	Path     []string
	Nullable bool
	Export   *FieldExport `json:"export,omitempty"`
}

func (_ *Boolean) NodeKind() NodeKind {
	return NodeKindBoolean
}

func (e *Boolean) NodePath() []string {
	return e.Path
}

func (e *Boolean) NodeZeroValue(bool) []byte {
	if e.Nullable {
		return nil
	}
	return literal.FALSE
}

func (e *Boolean) ExportedVariables() (variables []string) {
	if e.Export != nil {
		variables = append(variables, e.Export.Path[0])
	}
	return
}

type Float struct {
	Path     []string
	Nullable bool
	Export   *FieldExport `json:"export,omitempty"`
}

func (_ *Float) NodeKind() NodeKind {
	return NodeKindFloat
}

func (e *Float) NodePath() []string {
	return e.Path
}

func (e *Float) NodeZeroValue(bool) []byte {
	if e.Nullable {
		return nil
	}
	return literal.ZeroNumberValue
}

func (e *Float) ExportedVariables() (variables []string) {
	if e.Export != nil {
		variables = append(variables, e.Export.Path[0])
	}
	return
}

type Integer struct {
	Path     []string
	Nullable bool
	Export   *FieldExport `json:"export,omitempty"`
}

func (_ *Integer) NodeKind() NodeKind {
	return NodeKindInteger
}

func (e *Integer) NodePath() []string {
	return e.Path
}

func (e *Integer) NodeZeroValue(bool) []byte {
	if e.Nullable {
		return nil
	}
	return literal.ZeroNumberValue
}

func (e *Integer) ExportedVariables() (variables []string) {
	if e.Export != nil {
		variables = append(variables, e.Export.Path[0])
	}
	return
}

type Array struct {
	Path                []string
	Nullable            bool
	ResolveAsynchronous bool
	Item                Node
	Stream              Stream
}

type Stream struct {
	Enabled          bool
	InitialBatchSize int
	PatchIndex       int
}

func (_ *Array) NodeKind() NodeKind {
	return NodeKindArray
}

func (e *Array) NodePath() []string {
	return e.Path
}

func (e *Array) NodeZeroValue(skipAll bool) []byte {
	if e.Nullable && !skipAll {
		return nil
	}
	return literal.ZeroArrayValue
}

func (e *Array) ExportedVariables() (variables []string) {
	if itemExport, ok := e.Item.(FieldExportVariable); ok {
		variables = append(variables, itemExport.ExportedVariables()...)
	}
	return
}

type GraphQLSubscription struct {
	Trigger  GraphQLSubscriptionTrigger
	Response *GraphQLResponse
}

type GraphQLSubscriptionTrigger struct {
	Input         []byte
	InputTemplate InputTemplate
	Variables     Variables
	Source        SubscriptionDataSource
}

type FlushWriter interface {
	io.Writer
	Flush()
}

type GraphQLResponse struct {
	Data            Node
	RenameTypeNames []RenameTypeName
}

type RenameTypeName struct {
	From, To []byte
}

type GraphQLStreamingResponse struct {
	InitialResponse *GraphQLResponse
	Patches         []*GraphQLResponsePatch
	FlushInterval   int64
}

type GraphQLResponsePatch struct {
	Value     Node
	Fetch     Fetch
	Operation []byte
}

type BufPair struct {
	Data   *fastbuffer.FastBuffer
	Errors *fastbuffer.FastBuffer
}

func NewBufPair() *BufPair {
	return &BufPair{
		Data:   fastbuffer.New(),
		Errors: fastbuffer.New(),
	}
}

func (b *BufPair) HasData() bool {
	return b.Data.Len() != 0
}

func (b *BufPair) HasErrors() bool {
	return b.Errors.Len() != 0
}

func (b *BufPair) Reset() {
	b.Data.Reset()
	b.Errors.Reset()
}

func (b *BufPair) writeErrors(data []byte) {
	b.Errors.WriteBytes(data)
}

func (b *BufPair) WriteErr(message, locations, path, extensions []byte) {
	if b.HasErrors() {
		b.writeErrors(comma)
	}
	b.writeErrors(lBrace)
	b.writeErrors(quote)
	b.writeErrors(literalMessage)
	b.writeErrors(quote)
	b.writeErrors(colon)
	b.writeErrors(quote)
	b.writeErrors(message)
	b.writeErrors(quote)

	if locations != nil {
		b.writeErrors(comma)
		b.writeErrors(quote)
		b.writeErrors(literalLocations)
		b.writeErrors(quote)
		b.writeErrors(colon)
		b.writeErrors(locations)
	}

	if path != nil {
		b.writeErrors(comma)
		b.writeErrors(quote)
		b.writeErrors(literalPath)
		b.writeErrors(quote)
		b.writeErrors(colon)
		b.writeErrors(path)
	}

	if extensions != nil {
		b.writeErrors(comma)
		b.writeErrors(quote)
		b.writeErrors(literalExtensions)
		b.writeErrors(quote)
		b.writeErrors(colon)
		b.writeErrors(extensions)
	}

	b.writeErrors(rBrace)
}

func (r *Resolver) MergeBufPairs(from, to *BufPair, prefixDataWithComma bool) {
	r.MergeBufPairData(from, to, prefixDataWithComma)
	r.MergeBufPairErrors(from, to)
}

func (r *Resolver) MergeBufPairData(from, to *BufPair, prefixDataWithComma bool) {
	if !from.HasData() {
		return
	}
	if prefixDataWithComma {
		to.Data.WriteBytes(comma)
	}
	to.Data.WriteBytes(from.Data.Bytes())
	from.Data.Reset()
}

func (r *Resolver) MergeBufPairErrors(from, to *BufPair) {
	if !from.HasErrors() {
		return
	}
	if to.HasErrors() {
		to.Errors.WriteBytes(comma)
	}
	to.Errors.WriteBytes(from.Errors.Bytes())
	from.Errors.Reset()
}

func (r *Resolver) freeBufPair(pair *BufPair) {
	pair.Data.Reset()
	pair.Errors.Reset()
	r.bufPairPool.Put(pair)
}

func (r *Resolver) getResultSet() *resultSet {
	return r.resultSetPool.Get().(*resultSet)
}

func (r *Resolver) getBufPair() *BufPair {
	return r.bufPairPool.Get().(*BufPair)
}

func (r *Resolver) getBufPairSlice() *[]*BufPair {
	return r.bufPairSlicePool.Get().(*[]*BufPair)
}

func (r *Resolver) freeBufPairSlice(slice *[]*BufPair) {
	for i := range *slice {
		r.freeBufPair((*slice)[i])
	}
	*slice = (*slice)[:0]
	r.bufPairSlicePool.Put(slice)
}

func (r *Resolver) getErrChan() chan error {
	return r.errChanPool.Get().(chan error)
}

func (r *Resolver) freeErrChan(ch chan error) {
	r.errChanPool.Put(ch)
}

func (r *Resolver) getWaitGroup() *sync.WaitGroup {
	return r.waitGroupPool.Get().(*sync.WaitGroup)
}

func (r *Resolver) freeWaitGroup(wg *sync.WaitGroup) {
	r.waitGroupPool.Put(wg)
}

func writeGraphqlResponse(buf *BufPair, writer io.Writer, ignoreData bool) (err error) {
	hasErrors := buf.Errors.Len() != 0
	hasData := buf.Data.Len() != 0 && !ignoreData

	err = writeSafe(err, writer, lBrace)

	if hasErrors {
		err = writeSafe(err, writer, quote)
		err = writeSafe(err, writer, literalErrors)
		err = writeSafe(err, writer, quote)
		err = writeSafe(err, writer, colon)
		err = writeSafe(err, writer, lBrack)
		err = writeSafe(err, writer, buf.Errors.Bytes())
		err = writeSafe(err, writer, rBrack)
		err = writeSafe(err, writer, comma)
	}

	err = writeSafe(err, writer, quote)
	err = writeSafe(err, writer, literalData)
	err = writeSafe(err, writer, quote)
	err = writeSafe(err, writer, colon)

	if hasData {
		_, err = writer.Write(buf.Data.Bytes())
	} else {
		err = writeSafe(err, writer, literal.NULL)
	}
	err = writeSafe(err, writer, rBrace)

	return err
}

func writeSafe(err error, writer io.Writer, data []byte) error {
	if err != nil {
		return err
	}
	_, err = writer.Write(data)
	return err
}
