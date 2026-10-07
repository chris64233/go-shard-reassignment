package goshardreassignment

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// persistedState 是 Manager 的全部可持久化状态。每次变更整体原子落盘，
// 保证归属改写与任务阶段推进属于同一个持久化事务，崩溃后不会出现
// “归属已改但任务未完成”之类的中间态。
type persistedState struct {
	Tasks    map[string]*Task   `json:"tasks"`
	Owners   map[string]string  `json:"owners"`
	Requests map[string]string  `json:"requests"`
	Replans  map[string]*Replan `json:"replans"`
	Audit    []AuditEntry       `json:"audit"`
}

func newPersistedState() *persistedState {
	return &persistedState{
		Tasks:    map[string]*Task{},
		Owners:   map[string]string{},
		Requests: map[string]string{},
		Replans:  map[string]*Replan{},
	}
}

// Store 是持久化检查点存储。
type Store interface {
	Load() (*persistedState, error)
	Save(s *persistedState) error
}

// MemoryStore 是内存实现，主要用于测试。
type MemoryStore struct {
	mu    sync.Mutex
	state *persistedState
}

// NewMemoryStore 创建空的内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{state: newPersistedState()}
}

// Load 返回内存状态的深拷贝。
func (m *MemoryStore) Load() (*persistedState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneState(m.state), nil
}

// Save 以深拷贝方式保存状态。
func (m *MemoryStore) Save(s *persistedState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = cloneState(s)
	return nil
}

// FileStore 以 JSON 文件持久化状态，写入采用临时文件 + rename 保证原子性。
type FileStore struct {
	mu   sync.Mutex
	path string
}

// NewFileStore 创建基于给定路径的文件存储。
func NewFileStore(path string) *FileStore {
	return &FileStore{path: path}
}

// Load 读取文件状态；文件不存在时返回空状态。
func (f *FileStore) Load() (*persistedState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := os.ReadFile(f.path)
	if errors.Is(err, fs.ErrNotExist) {
		return newPersistedState(), nil
	}
	if err != nil {
		return nil, err
	}
	state := newPersistedState()
	if err := json.Unmarshal(data, state); err != nil {
		return nil, fmt.Errorf("decode store %s: %w", f.path, err)
	}
	return state, nil
}

// Save 原子写入状态文件（临时文件 + rename + 目录 fsync）。
func (f *FileStore) Save(s *persistedState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return err
	}
	if dir, err := os.Open(filepath.Dir(f.path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

func cloneState(s *persistedState) *persistedState {
	out := newPersistedState()
	for k, v := range s.Tasks {
		task := *v
		if v.PendingReplan != nil {
			replan := *v.PendingReplan
			task.PendingReplan = &replan
		}
		out.Tasks[k] = &task
	}
	for k, v := range s.Owners {
		out.Owners[k] = v
	}
	for k, v := range s.Requests {
		out.Requests[k] = v
	}
	for k, v := range s.Replans {
		replan := *v
		out.Replans[k] = &replan
	}
	out.Audit = append(out.Audit, s.Audit...)
	return out
}
