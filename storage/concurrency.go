package storage

import (
	"context"
	"fmt"
	"sync"
)

type lockEntry struct {
	exclusive chan struct{}
	shared    int
}

func newLockEntry() *lockEntry {
	e := &lockEntry{
		exclusive: make(chan struct{}, 1),
		shared:    0,
	}
	e.exclusive <- struct{}{}
	return e
}

type lockTable struct {
	locks *sync.Map
	mu    sync.Mutex
}

func newLockTable() *lockTable {
	return &lockTable{
		locks: &sync.Map{},
		mu:    sync.Mutex{},
	}
}

func (t *lockTable) sLock(ctx context.Context, blk BlockIDHash) error {
	var e *lockEntry
	{
		v, ok := t.locks.Load(blk)
		if !ok {
			e = t.addEntry(blk)
		} else {
			e = v.(*lockEntry)
		}
	}
	select {
	case <-e.exclusive:
		e.shared++
	case <-ctx.Done():
		return fmt.Errorf("sLock is canceled: %w", ctx.Err())
	}
	e.exclusive <- struct{}{}
	return nil
}

func (t *lockTable) xLock(ctx context.Context, blk BlockIDHash) error {
	var e *lockEntry
	{
		v, ok := t.locks.Load(blk)
		if !ok {
			e = t.addEntry(blk)
		} else {
			e = v.(*lockEntry)
		}
	}
	select {
	case <-e.exclusive:
	case <-ctx.Done():
		return fmt.Errorf("xLock is canceled: %w", ctx.Err())
	}
	return nil
}

func (t *lockTable) sUnlock(blk BlockIDHash) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var e *lockEntry
	{
		v, ok := t.locks.Load(blk)
		if !ok {
			return
		} else {
			e = v.(*lockEntry)
		}
	}
	if e.shared > 0 {
		e.shared--
	}
	if len(e.exclusive) > 0 && e.shared == 0 {
		t.locks.Delete(blk)
	}
}

func (t *lockTable) xUnlock(blk BlockIDHash) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var e *lockEntry
	{
		v, ok := t.locks.Load(blk)
		if !ok {
			return
		} else {
			e = v.(*lockEntry)
		}
	}
	if len(e.exclusive) > 0 && e.shared == 0 {
		t.locks.Delete(blk)
	}
	if len(e.exclusive) == 0 {
		e.exclusive <- struct{}{}
	}
}

func (t *lockTable) addEntry(blk BlockIDHash) *lockEntry {
	t.mu.Lock()
	defer t.mu.Unlock()

	v, ok := t.locks.Load(blk)
	if ok {
		return v.(*lockEntry)
	}
	e := newLockEntry()
	t.locks.Store(blk, e)
	return e
}

type concurrencyManager struct {
	lockTab *lockTable
	locks   map[BlockIDHash]string
}

func newConcurrencyManager(lockTab *lockTable) *concurrencyManager {
	return &concurrencyManager{
		lockTab: lockTab,
		locks:   map[BlockIDHash]string{},
	}
}

func (m *concurrencyManager) sLock(ctx context.Context, blk BlockIDHash) error {
	_, ok := m.locks[blk]
	if ok {
		return nil
	}
	err := m.lockTab.sLock(ctx, blk)
	if err != nil {
		return err
	}
	m.locks[blk] = "s"
	return nil
}

func (m *concurrencyManager) xLock(ctx context.Context, blk BlockIDHash) error {
	if m.xLocked(blk) {
		return nil
	}
	err := m.sLock(ctx, blk)
	if err != nil {
		return err
	}
	err = m.lockTab.xLock(ctx, blk)
	if err != nil {
		return err
	}
	m.locks[blk] = "x"
	return nil
}

func (m *concurrencyManager) release() {
	for blk, l := range m.locks {
		if l == "s" {
			m.lockTab.sUnlock(blk)
		}
		if l == "x" {
			m.lockTab.sUnlock(blk)
			m.lockTab.xUnlock(blk)
		}
	}
	m.locks = map[BlockIDHash]string{}
}

func (m *concurrencyManager) xLocked(blk BlockIDHash) bool {
	l, ok := m.locks[blk]
	if ok && l == "x" {
		return true
	}
	return false
}
