package balance

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	rendezvous "github.com/dgryski/go-rendezvous"
	"github.com/zhuud/go-library/utils"
)

type NodeBalanceProvider interface {
	// GetPinnedNode 返回 id 对应的固定节点。ok=false 表示没有固定映射。
	GetPinnedNode(ctx context.Context, id string) (node string, ok bool, err error)
	// GetNodeList 返回当前可用节点列表（调用方实现节点来源与更新逻辑）。
	GetNodeList(ctx context.Context) ([]string, error)
}

type NodeBalanceConfig struct {
	FallbackWhenPinnedInvalid bool
	HashFunc                  func(key string) uint64
}

type NodeBalanceOption func(cfg *NodeBalanceConfig)

type NodeBalance struct {
	provider                  NodeBalanceProvider
	hashFunc                  func(key string) uint64
	fallbackWhenPinnedInvalid bool

	mu        sync.RWMutex
	ring      *rendezvous.Rendezvous
	nodeSet   map[string]struct{}
	nodesHash uint64
}

func NewNodeBalance(provider NodeBalanceProvider, opts ...NodeBalanceOption) (*NodeBalance, error) {
	if provider == nil {
		return nil, fmt.Errorf("utils.NewNodeBalance.ValidateParams provider 不能为空")
	}

	cfg := NodeBalanceConfig{
		FallbackWhenPinnedInvalid: true,
		HashFunc:                  fnv1aHash,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	return &NodeBalance{
		provider:                  provider,
		hashFunc:                  cfg.HashFunc,
		fallbackWhenPinnedInvalid: cfg.FallbackWhenPinnedInvalid,
		nodeSet:                   make(map[string]struct{}),
	}, nil
}

// Locate 根据 id 返回路由节点：固定映射优先，未命中或降级时走一致性哈希。
func (n *NodeBalance) Locate(ctx context.Context, id string) (string, error) {
	trimId := strings.TrimSpace(id)
	if trimId == "" {
		return "", fmt.Errorf("utils.NodeBalance.Locate.ValidateParams id 不能为空")
	}

	nodes, err := n.provider.GetNodeList(ctx)
	if err != nil {
		return "", fmt.Errorf("utils.NodeBalance.GetNodeList error: %w", err)
	}
	if err = n.refresh(nodes); err != nil {
		return "", err
	}

	pinnedNode, ok, err := n.provider.GetPinnedNode(ctx, trimId)
	if err != nil {
		return "", fmt.Errorf("utils.NodeBalance.GetPinnedNode id:%s, error: %w", trimId, err)
	}
	if ok {
		pinnedNode = strings.TrimSpace(pinnedNode)
		if n.isNodeValid(pinnedNode) {
			return pinnedNode, nil
		}
		if !n.fallbackWhenPinnedInvalid {
			return "", fmt.Errorf("utils.NodeBalance.PinnedNodeInvalid id:%s, node:%s", trimId, pinnedNode)
		}
	}

	n.mu.RLock()
	node := n.ring.Lookup(trimId)
	n.mu.RUnlock()
	return node, nil
}

func (n *NodeBalance) isNodeValid(node string) bool {
	n.mu.RLock()
	_, ok := n.nodeSet[node]
	n.mu.RUnlock()
	return ok
}

func (n *NodeBalance) refresh(rawNodes []string) error {
	if len(rawNodes) == 0 {
		return fmt.Errorf("utils.NodeBalance.refresh.ValidateNodes 节点列表不能为空")
	}

	normalized := make([]string, 0, len(rawNodes))
	for _, node := range rawNodes {
		trimNode := strings.TrimSpace(node)
		normalized = append(normalized, trimNode)
	}
	nodes := utils.ArrayUnique(normalized)
	if len(nodes) == 0 {
		return fmt.Errorf("utils.NodeBalance.refresh.ValidateNodes 节点列表不能为空")
	}
	sort.Strings(nodes)

	nodesHash := hashNodeList(nodes)

	n.mu.RLock()
	if n.ring != nil && n.nodesHash == nodesHash {
		n.mu.RUnlock()
		return nil
	}
	n.mu.RUnlock()

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.ring != nil && n.nodesHash == nodesHash {
		return nil
	}

	nodeSet := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		nodeSet[node] = struct{}{}
	}

	n.ring = rendezvous.New(nodes, n.hashFunc)
	n.nodeSet = nodeSet
	n.nodesHash = nodesHash
	return nil
}

func hashNodeList(nodes []string) uint64 {
	var hash uint64 = 1469598103934665603
	for _, node := range nodes {
		hash = fnv1aHash(node)
		hash ^= uint64('|')
		hash *= 1099511628211
	}
	return hash
}

func fnv1aHash(key string) uint64 {
	var hash uint64 = 1469598103934665603
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= 1099511628211
	}
	return hash
}

func WithNodeBalanceFallbackWhenPinnedInvalid(fallback bool) NodeBalanceOption {
	return func(cfg *NodeBalanceConfig) {
		cfg.FallbackWhenPinnedInvalid = fallback
	}
}

func WithNodeBalanceHashFunc(hashFunc func(key string) uint64) NodeBalanceOption {
	return func(cfg *NodeBalanceConfig) {
		if hashFunc != nil {
			cfg.HashFunc = hashFunc
		}
	}
}
