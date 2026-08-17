// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import "sync"

// actorLocks serializes node-local operations that would otherwise run against
// one actor's on-node state concurrently.
//
// Recovering a lost checkpoint response makes a re-entered attempt succeed
// rather than fail, which is the point — but it also means two attempts at one
// actor can now both reach the teardown that follows a checkpoint. That
// teardown removes the actor's checkpoint dir, which is the source the other
// attempt may still be uploading from. Recognizing a repeated operation and
// excluding a concurrent one are separate problems, and solving the first does
// not solve the second.
//
// The zero value is ready to use. Entries are dropped on release, so this
// holds one entry per in-flight operation rather than one per actor the node
// has ever seen.
type actorLocks struct {
	mu   sync.Mutex
	held map[string]struct{}
}

// tryLock claims actorUID for the caller, reporting whether it got it. It does
// not wait: a caller that finds the actor busy has nothing useful to do
// meanwhile, since the operation it would run is the one already in progress.
//
// release is always non-nil, so `defer release()` is safe on either outcome,
// and is idempotent.
func (l *actorLocks) tryLock(actorUID string) (release func(), ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, busy := l.held[actorUID]; busy {
		return func() {}, false
	}
	if l.held == nil {
		l.held = map[string]struct{}{}
	}
	l.held[actorUID] = struct{}{}
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			delete(l.held, actorUID)
		})
	}, true
}
