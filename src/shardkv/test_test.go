package shardkv

import "raft-shard-kv/src/porcupine"
import "raft-shard-kv/src/models"
import "testing"
import "strconv"
import "time"
import "fmt"
import "sync/atomic"
import "sync"
import "math/rand"
import "io/ioutil"
import "sort"

const linearizabilityCheckTimeout = 1 * time.Second

func check(t *testing.T, ck *Clerk, key string, value string) {
	v := ck.Get(key)
	if v != value {
		t.Fatalf("Get(%v): expected:\n%v\nreceived:\n%v", key, value, v)
	}
}

// test static 2-way sharding, without shard movement.
func TestStaticShards(t *testing.T) {
	fmt.Printf("Test: static shards ...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)
	cfg.join(1)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	// make sure that the data really is sharded by
	// shutting down one shard and checking that some
	// Get()s don't succeed.
	cfg.ShutdownGroup(1)
	cfg.checklogs() // forbid snapshots

	ch := make(chan string)
	for xi := 0; xi < n; xi++ {
		ck1 := cfg.makeClient() // only one call allowed per client
		go func(i int) {
			v := ck1.Get(ka[i])
			if v != va[i] {
				ch <- fmt.Sprintf("Get(%v): expected:\n%v\nreceived:\n%v", ka[i], va[i], v)
			} else {
				ch <- ""
			}
		}(xi)
	}

	// wait a bit, only about half the Gets should succeed.
	ndone := 0
	done := false
	for done == false {
		select {
		case err := <-ch:
			if err != "" {
				t.Fatal(err)
			}
			ndone += 1
		case <-time.After(time.Second * 2):
			done = true
			break
		}
	}

	if ndone != 5 {
		t.Fatalf("expected 5 completions with one shard dead; got %v\n", ndone)
	}

	// bring the crashed shard/group back to life.
	cfg.StartGroup(1)
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestJoinLeave(t *testing.T) {
	fmt.Printf("Test: join then leave ...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	cfg.join(1)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(5)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.leave(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(5)
		ck.Append(ka[i], x)
		va[i] += x
	}

	// allow time for shards to transfer.
	time.Sleep(1 * time.Second)

	cfg.checklogs()
	cfg.ShutdownGroup(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestSnapshot(t *testing.T) {
	fmt.Printf("Test: snapshots, join, and leave ...\n")

	cfg := make_config(t, 3, false, 1000)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 30
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	cfg.join(1)
	cfg.join(2)
	cfg.leave(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.leave(1)
	cfg.join(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	time.Sleep(1 * time.Second)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	time.Sleep(1 * time.Second)

	cfg.checklogs()

	cfg.ShutdownGroup(0)
	cfg.ShutdownGroup(1)
	cfg.ShutdownGroup(2)

	cfg.StartGroup(0)
	cfg.StartGroup(1)
	cfg.StartGroup(2)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestMissChange(t *testing.T) {
	fmt.Printf("Test: servers miss configuration changes...\n")

	cfg := make_config(t, 3, false, 1000)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(20)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	cfg.join(1)

	cfg.ShutdownServer(0, 0)
	cfg.ShutdownServer(1, 0)
	cfg.ShutdownServer(2, 0)

	cfg.join(2)
	cfg.leave(1)
	cfg.leave(0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.join(1)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.StartServer(0, 0)
	cfg.StartServer(1, 0)
	cfg.StartServer(2, 0)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	time.Sleep(2 * time.Second)

	cfg.ShutdownServer(0, 1)
	cfg.ShutdownServer(1, 1)
	cfg.ShutdownServer(2, 1)

	cfg.join(0)
	cfg.leave(2)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.StartServer(0, 1)
	cfg.StartServer(1, 1)
	cfg.StartServer(2, 1)

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestConcurrent1(t *testing.T) {
	fmt.Printf("Test: concurrent puts and configuration changes...\n")

	cfg := make_config(t, 3, false, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(5)
			ck1.Append(ka[i], x)
			va[i] += x
			time.Sleep(10 * time.Millisecond)
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)

	cfg.ShutdownGroup(0)
	time.Sleep(100 * time.Millisecond)
	cfg.ShutdownGroup(1)
	time.Sleep(100 * time.Millisecond)
	cfg.ShutdownGroup(2)

	cfg.leave(2)

	time.Sleep(100 * time.Millisecond)
	cfg.StartGroup(0)
	cfg.StartGroup(1)
	cfg.StartGroup(2)

	time.Sleep(100 * time.Millisecond)
	cfg.join(0)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)

	time.Sleep(1 * time.Second)

	atomic.StoreInt32(&done, 1)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

// this tests the various sources from which a re-starting
// group might need to fetch shard contents.
func TestConcurrent2(t *testing.T) {
	fmt.Printf("Test: more concurrent puts and configuration changes...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(1)
	cfg.join(0)
	cfg.join(2)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(1)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int, ck1 *Clerk) {
		defer func() { ch <- true }()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(1)
			ck1.Append(ka[i], x)
			va[i] += x
			time.Sleep(50 * time.Millisecond)
		}
	}

	for i := 0; i < n; i++ {
		ck1 := cfg.makeClient()
		go ff(i, ck1)
	}

	cfg.leave(0)
	cfg.leave(2)
	time.Sleep(3000 * time.Millisecond)
	cfg.join(0)
	cfg.join(2)
	cfg.leave(1)
	time.Sleep(3000 * time.Millisecond)
	cfg.join(1)
	cfg.leave(0)
	cfg.leave(2)
	time.Sleep(3000 * time.Millisecond)

	cfg.ShutdownGroup(1)
	cfg.ShutdownGroup(2)
	time.Sleep(1000 * time.Millisecond)
	cfg.StartGroup(1)
	cfg.StartGroup(2)

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestConcurrent3(t *testing.T) {
	fmt.Printf("Test: concurrent configuration change and restart...\n")

	cfg := make_config(t, 3, false, 300)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i)
		va[i] = randstring(1)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int, ck1 *Clerk) {
		defer func() { ch <- true }()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(1)
			ck1.Append(ka[i], x)
			va[i] += x
		}
	}

	for i := 0; i < n; i++ {
		ck1 := cfg.makeClient()
		go ff(i, ck1)
	}

	t0 := time.Now()
	for time.Since(t0) < 12*time.Second {
		cfg.join(2)
		cfg.join(1)
		time.Sleep(time.Duration(rand.Int()%900) * time.Millisecond)
		cfg.ShutdownGroup(0)
		cfg.ShutdownGroup(1)
		cfg.ShutdownGroup(2)
		cfg.StartGroup(0)
		cfg.StartGroup(1)
		cfg.StartGroup(2)

		time.Sleep(time.Duration(rand.Int()%900) * time.Millisecond)
		cfg.leave(1)
		cfg.leave(2)
		time.Sleep(time.Duration(rand.Int()%900) * time.Millisecond)
	}

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestUnreliable1(t *testing.T) {
	fmt.Printf("Test: unreliable 1...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	cfg.join(1)
	cfg.join(2)
	cfg.leave(0)

	for ii := 0; ii < n*2; ii++ {
		i := ii % n
		check(t, ck, ka[i], va[i])
		x := randstring(5)
		ck.Append(ka[i], x)
		va[i] += x
	}

	cfg.join(0)
	cfg.leave(1)

	for ii := 0; ii < n*2; ii++ {
		i := ii % n
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestUnreliable2(t *testing.T) {
	fmt.Printf("Test: unreliable 2...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(5)
			ck1.Append(ka[i], x)
			va[i] += x
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)
	cfg.join(0)

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	cfg.net.Reliable(true)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestUnreliable3(t *testing.T) {
	fmt.Printf("Test: unreliable 3...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	begin := time.Now()
	var operations []porcupine.Operation
	var opMu sync.Mutex

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		start := int64(time.Since(begin))
		ck.Put(ka[i], va[i])
		end := int64(time.Since(begin))
		inp := models.KvInput{Op: 1, Key: ka[i], Value: va[i]}
		var out models.KvOutput
		op := porcupine.Operation{Input: inp, Call: start, Output: out, Return: end, ClientId: 0}
		operations = append(operations, op)
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient()
		for atomic.LoadInt32(&done) == 0 {
			ki := rand.Int() % n
			nv := randstring(5)
			var inp models.KvInput
			var out models.KvOutput
			start := int64(time.Since(begin))
			if (rand.Int() % 1000) < 500 {
				ck1.Append(ka[ki], nv)
				inp = models.KvInput{Op: 2, Key: ka[ki], Value: nv}
			} else if (rand.Int() % 1000) < 100 {
				ck1.Put(ka[ki], nv)
				inp = models.KvInput{Op: 1, Key: ka[ki], Value: nv}
			} else {
				v := ck1.Get(ka[ki])
				inp = models.KvInput{Op: 0, Key: ka[ki]}
				out = models.KvOutput{Value: v}
			}
			end := int64(time.Since(begin))
			op := porcupine.Operation{Input: inp, Call: start, Output: out, Return: end, ClientId: i}
			opMu.Lock()
			operations = append(operations, op)
			opMu.Unlock()
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)
	cfg.join(0)

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	cfg.net.Reliable(true)
	for i := 0; i < n; i++ {
		<-ch
	}

	res, info := porcupine.CheckOperationsVerbose(models.KvModel, operations, linearizabilityCheckTimeout)
	if res == porcupine.Illegal {
		file, err := ioutil.TempFile("", "*.html")
		if err != nil {
			fmt.Printf("info: failed to create temp file for visualization")
		} else {
			err = porcupine.Visualize(models.KvModel, info, file)
			if err != nil {
				fmt.Printf("info: failed to write history visualization to %s\n", file.Name())
			} else {
				fmt.Printf("info: wrote history visualization to %s\n", file.Name())
			}
		}
		t.Fatal("history is not linearizable")
	} else if res == porcupine.Unknown {
		fmt.Println("info: linearizability check timed out, assuming history is ok")
	}

	fmt.Printf("  ... Passed\n")
}

// optional test to see whether servers are deleting
// shards for which they are no longer responsible.
func TestChallenge1Delete(t *testing.T) {
	fmt.Printf("Test: shard deletion (challenge 1) ...\n")

	// "1" means force snapshot after every log entry.
	cfg := make_config(t, 3, false, 1)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	// 30,000 bytes of total values.
	n := 30
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i)
		va[i] = randstring(1000)
		ck.Put(ka[i], va[i])
	}
	for i := 0; i < 3; i++ {
		check(t, ck, ka[i], va[i])
	}

	for iters := 0; iters < 2; iters++ {
		cfg.join(1)
		cfg.leave(0)
		cfg.join(2)
		time.Sleep(3 * time.Second)
		for i := 0; i < 3; i++ {
			check(t, ck, ka[i], va[i])
		}
		cfg.leave(1)
		cfg.join(0)
		cfg.leave(2)
		time.Sleep(3 * time.Second)
		for i := 0; i < 3; i++ {
			check(t, ck, ka[i], va[i])
		}
	}

	cfg.join(1)
	cfg.join(2)
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		check(t, ck, ka[i], va[i])
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		check(t, ck, ka[i], va[i])
	}
	time.Sleep(1 * time.Second)
	for i := 0; i < 3; i++ {
		check(t, ck, ka[i], va[i])
	}

	total := 0
	for gi := 0; gi < cfg.ngroups; gi++ {
		for i := 0; i < cfg.n; i++ {
			raft := cfg.groups[gi].saved[i].RaftStateSize()
			snap := len(cfg.groups[gi].saved[i].ReadSnapshot())
			total += raft + snap
		}
	}

	// 27 keys should be stored once.
	// 3 keys should also be stored in client dup tables.
	// everything on 3 replicas.
	// plus slop.
	expected := 3 * (((n - 3) * 1000) + 2*3*1000 + 6000)
	if total > expected {
		t.Fatalf("snapshot + persisted Raft state are too big: %v > %v\n", total, expected)
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

// optional test to see whether servers can handle
// shards that are not affected by a config change
// while the config change is underway
func TestChallenge2Unaffected(t *testing.T) {
	fmt.Printf("Test: unaffected shard access (challenge 2) ...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	// JOIN 100
	cfg.join(0)

	// Do a bunch of puts to keys in all shards
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = "100"
		ck.Put(ka[i], va[i])
	}

	// JOIN 101
	cfg.join(1)

	// QUERY to find shards now owned by 101
	c := cfg.mck.Query(-1)
	owned := make(map[int]bool, n)
	for s, gid := range c.Shards {
		owned[s] = gid == cfg.groups[1].gid
	}

	// Wait for migration to new config to complete, and for clients to
	// start using this updated config. Gets to any key k such that
	// owned[shard(k)] == true should now be served by group 101.
	<-time.After(1 * time.Second)
	for i := 0; i < n; i++ {
		if owned[i] {
			va[i] = "101"
			ck.Put(ka[i], va[i])
		}
	}

	// KILL 100
	cfg.ShutdownGroup(0)

	// LEAVE 100
	// 101 doesn't get a chance to migrate things previously owned by 100
	cfg.leave(0)

	// Wait to make sure clients see new config
	<-time.After(1 * time.Second)

	// And finally: check that gets/puts for 101-owned keys still complete
	for i := 0; i < n; i++ {
		shard := int(ka[i][0]) % 10
		if owned[shard] {
			check(t, ck, ka[i], va[i])
			ck.Put(ka[i], va[i]+"-1")
			check(t, ck, ka[i], va[i]+"-1")
		}
	}

	fmt.Printf("  ... Passed\n")
}

// optional test to see whether servers can handle operations on shards that
// have been received as a part of a config migration when the entire migration
// has not yet completed.
func TestChallenge2Partial(t *testing.T) {
	fmt.Printf("Test: partial migration shard access (challenge 2) ...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	// JOIN 100 + 101 + 102
	cfg.joinm([]int{0, 1, 2})

	// Give the implementation some time to reconfigure
	<-time.After(1 * time.Second)

	// Do a bunch of puts to keys in all shards
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = "100"
		ck.Put(ka[i], va[i])
	}

	// QUERY to find shards owned by 102
	c := cfg.mck.Query(-1)
	owned := make(map[int]bool, n)
	for s, gid := range c.Shards {
		owned[s] = gid == cfg.groups[2].gid
	}

	// KILL 100
	cfg.ShutdownGroup(0)

	// LEAVE 100 + 102
	// 101 can get old shards from 102, but not from 100. 101 should start
	// serving shards that used to belong to 102 as soon as possible
	cfg.leavem([]int{0, 2})

	// Give the implementation some time to start reconfiguration
	// And to migrate 102 -> 101
	<-time.After(1 * time.Second)

	// And finally: check that gets/puts for 101-owned keys now complete
	for i := 0; i < n; i++ {
		shard := key2shard(ka[i])
		if owned[shard] {
			check(t, ck, ka[i], va[i])
			ck.Put(ka[i], va[i]+"-2")
			check(t, ck, ka[i], va[i]+"-2")
		}
	}

	fmt.Printf("  ... Passed\n")
}

func TestThroughput(t *testing.T) {
	fmt.Printf("Test: Throughput measurement ...\n")

	// 1. Test Setup
	numGroups := 3
	serversPerGroup := 3
	cfg := make_config(t, serversPerGroup, false, 10000) // Reliable network
	defer cfg.cleanup()

	for i := 0; i < numGroups; i++ {
		cfg.join(i)
	}
	// cfg.joinm(gids) // Or join all at once if supported
	fmt.Printf("Waiting for cluster to stabilize after joining %d groups...\n", numGroups)
	time.Sleep(3 * time.Second) // Give time for configuration propagation and leader election

	// 2. Concurrency Setup
	numClients := 3 // Number of concurrent goroutines (clients)
	testDuration := 30 * time.Second

	var opsCount int64 // Atomic counter for successful operations
	var wg sync.WaitGroup
	stopCh := make(chan struct{}) // Channel to signal stop

	// 3. Start Concurrent Workload
	wg.Add(numClients)
	startTime := time.Now()

	for i := 0; i < numClients; i++ {
		go func(id int) {
			defer wg.Done()
			ck := cfg.makeClient() // Each goroutine gets its own client

			for {
				select {
				case <-stopCh: // Check if we need to stop
					return
				default:
					// Generate workload
					key := strconv.Itoa(rand.Intn(1000)) // Example: keys from 0 to 9999
					val := randstring(64)                // Example: 64-byte values

					// Perform operation (e.g., 80% Get, 20% Put)
					opType := rand.Intn(100)

					if opType < 80 { // 80% Get
						ck.Get(key)
					} else { // 20% Put
						ck.Put(key, val)
					}

					// 4. Count successful operations
					atomic.AddInt64(&opsCount, 1)

					// Optional: Add a small delay or vary workload intensity
					// time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)))
				}
			}
		}(i)
	}

	// Let the test run for the specified duration
	time.Sleep(testDuration)

	// 5. Signal stop and wait for completion
	close(stopCh) // Signal all goroutines to stop
	fmt.Println("Stopping clients...")
	wg.Wait() // Wait for all goroutines to finish
	fmt.Println("All clients stopped.")

	elapsed := time.Since(startTime)
	totalOps := atomic.LoadInt64(&opsCount)

	// 6. Calculate and Report Results
	throughput := float64(totalOps) / elapsed.Seconds()

	fmt.Printf("--------------------\n")
	fmt.Printf("Throughput Test Results:\n")
	fmt.Printf("  Groups: %d\n", numGroups)
	fmt.Printf("  Servers/Group: %d\n", serversPerGroup)
	fmt.Printf("  Concurrent Clients: %d\n", numClients)
	fmt.Printf("  Duration: %v\n", elapsed.Round(time.Second))
	fmt.Printf("  Total Successful Ops: %d\n", totalOps)
	fmt.Printf("  Throughput: %.2f Ops/Sec\n", throughput)
	fmt.Printf("--------------------\n")
	t.Logf("Throughput: %.2f Ops/Sec", throughput) // Log for test summary

	fmt.Printf("  ... Passed\n") // Or add assertions if needed
}

func TestShardKVPerformance(t *testing.T) {
	fmt.Printf("测试：ShardKV 性能测试\n")

	// 测试参数定义
	type TestCase struct {
		name           string        // 测试名称
		system         string        // "shardkv" 或 "kvraft"
		numGroups      int           // 仅用于 shardkv
		serversPerNode int           // 每个节点（组）的服务器数量
		numClients     int           // 并发客户端数量
		duration       time.Duration // 测试持续时间
		valueSize      int           // 值大小（字节）
		readPercentage int           // 读操作百分比 (0-100)
		keyRange       int           // 键的范围 (0 to keyRange-1)
	}

	// 定义测试用例
	testCases := []TestCase{
		{"ShardKV-basic", "shardkv", 3, 3, 5, 20 * time.Second, 64, 80, 100},
		{"ShardKV-read-intensively", "shardkv", 3, 3, 5, 20 * time.Second, 64, 95, 100},
		{"ShardKV-write-intensively", "shardkv", 3, 3, 5, 20 * time.Second, 64, 50, 100},
		{"ShardKV-3nodes-1group", "shardkv", 1, 3, 5, 20 * time.Second, 64, 80, 100},
		{"ShardKV-6nodes-2group", "shardkv", 2, 3, 5, 20 * time.Second, 64, 80, 100},
		{"ShardKV-9nodes-3group", "shardkv", 3, 3, 5, 20 * time.Second, 64, 80, 100},
	}

	type TestResult struct {
		throughput   float64
		avgLatency   time.Duration
		p50Latency   time.Duration
		p99Latency   time.Duration
		readLatency  time.Duration
		writeLatency time.Duration
	}

	results := make(map[string]TestResult)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fmt.Printf("========== 开始测试: %s ==========\n", tc.name)

			var shardCfg *config
			shardCfg = make_config(t, tc.serversPerNode, false, 10000) // 可靠网络
			defer shardCfg.cleanup()

			// 加入所有组
			for i := 0; i < tc.numGroups; i++ {
				shardCfg.join(i)
			}
			fmt.Printf("等待集群稳定（加入 %d 个组）...\n", tc.numGroups)

			// 给集群一些时间以完成配置和选举
			time.Sleep(3 * time.Second)

			// 并发设置
			var opsCount int64           // 操作成功计数器
			var getCount, putCount int64 // 分别计数读写操作

			// 延迟统计
			var totalLatency, readLatency, writeLatency int64
			latencies := make([]int64, 0, 100000) // 用于计算百分位延迟
			var latencyMutex sync.Mutex

			var wg sync.WaitGroup
			stopCh := make(chan struct{}) // 停止信号通道

			// 启动并发工作负载
			wg.Add(tc.numClients)
			startTime := time.Now()

			for i := 0; i < tc.numClients; i++ {
				go func(id int) {
					defer wg.Done()

					// 根据系统类型创建对应的客户端
					var shardCk *Clerk
					shardCk = shardCfg.makeClient()

					for {
						select {
						case <-stopCh: // 检查是否应该停止
							return
						default:
							// 生成工作负载
							key := strconv.Itoa(rand.Intn(tc.keyRange))
							val := randstring(tc.valueSize)

							opType := rand.Intn(100)
							start := time.Now()

							if opType < tc.readPercentage {
								shardCk.Get(key)
								atomic.AddInt64(&getCount, 1)
								opLatency := time.Since(start).Nanoseconds()
								atomic.AddInt64(&readLatency, opLatency)
							} else {
								shardCk.Put(key, val)
								atomic.AddInt64(&putCount, 1)
								opLatency := time.Since(start).Nanoseconds()
								atomic.AddInt64(&writeLatency, opLatency)
							}

							opLatency := time.Since(start).Nanoseconds()
							atomic.AddInt64(&totalLatency, opLatency)

							latencyMutex.Lock()
							latencies = append(latencies, opLatency)
							latencyMutex.Unlock()

							// 计数成功操作
							atomic.AddInt64(&opsCount, 1)
						}
					}
				}(i)
			}

			time.Sleep(tc.duration)

			// 发送停止信号并等待完成
			close(stopCh)
			fmt.Println("正在停止客户端...")
			wg.Wait()
			fmt.Println("所有客户端已停止。")

			elapsed := time.Since(startTime)
			totalOps := atomic.LoadInt64(&opsCount)
			gets := atomic.LoadInt64(&getCount)
			puts := atomic.LoadInt64(&putCount)

			// 计算吞吐量
			throughput := float64(totalOps) / elapsed.Seconds()

			// 计算平均延迟
			var avgLatency time.Duration
			if totalOps > 0 {
				avgLatency = time.Duration(atomic.LoadInt64(&totalLatency) / totalOps)
			}

			// 计算读写延迟
			var avgReadLatency, avgWriteLatency time.Duration
			if gets > 0 {
				avgReadLatency = time.Duration(atomic.LoadInt64(&readLatency) / gets)
			}
			if puts > 0 {
				avgWriteLatency = time.Duration(atomic.LoadInt64(&writeLatency) / puts)
			}

			// 计算百分位延迟
			latencyMutex.Lock()
			sort.Slice(latencies, func(i, j int) bool {
				return latencies[i] < latencies[j]
			})

			var p50Latency, p99Latency time.Duration
			if len(latencies) > 0 {
				p50Index := len(latencies) * 50 / 100
				p99Index := len(latencies) * 99 / 100
				p50Latency = time.Duration(latencies[p50Index])
				p99Latency = time.Duration(latencies[p99Index])
			}
			latencyMutex.Unlock()

			// 保存结果用于比较
			results[tc.name] = TestResult{
				throughput:   throughput,
				avgLatency:   avgLatency,
				p50Latency:   p50Latency,
				p99Latency:   p99Latency,
				readLatency:  avgReadLatency,
				writeLatency: avgWriteLatency,
			}

			// 6. 输出结果
			fmt.Printf("--------------------\n")
			fmt.Printf("测试结果: %s\n", tc.name)
			fmt.Printf("  分片组数量: %d\n", tc.numGroups)
			fmt.Printf("  每节点服务器数: %d\n", tc.serversPerNode)
			fmt.Printf("  并发客户端数: %d\n", tc.numClients)
			fmt.Printf("  读/写比例: %d/%d\n", tc.readPercentage, 100-tc.readPercentage)
			fmt.Printf("  测试持续时间: %v\n", elapsed.Round(time.Second))
			fmt.Printf("  总操作数: %d (读: %d, 写: %d)\n", totalOps, gets, puts)
			fmt.Printf("  吞吐量: %.2f Ops/Sec\n", throughput)
			fmt.Printf("  平均延迟: %v\n", avgLatency)
			fmt.Printf("  P50延迟: %v\n", p50Latency)
			fmt.Printf("  P99延迟: %v\n", p99Latency)
			fmt.Printf("  读操作平均延迟: %v\n", avgReadLatency)
			fmt.Printf("  写操作平均延迟: %v\n", avgWriteLatency)
			fmt.Printf("--------------------\n")

			// 将主要结果记录到测试日志中
			t.Logf("%s - 吞吐量: %.2f Ops/Sec, 平均延迟: %v", tc.name, throughput, avgLatency)

			fmt.Printf("  ... 通过\n")
		})
	}

	// 输出汇总比较结果
	fmt.Printf("\n\n========== ShardKV 性能测试汇总 ==========\n")
	fmt.Printf("%-25s %-15s %-15s %-15s %-15s %-15s %-15s\n",
		"测试名称", "吞吐量(ops/sec)", "平均延迟", "P50延迟", "P99延迟", "读延迟", "写延迟")

	for _, tc := range testCases {
		result := results[tc.name]
		fmt.Printf("%-25s %-15.2f %-15v %-15v %-15v %-15v %-15v\n",
			tc.name, result.throughput, result.avgLatency,
			result.p50Latency, result.p99Latency,
			result.readLatency, result.writeLatency)
	}

	if result1, ok := results["1组"]; ok {
		if result3, ok := results["3组"]; ok {
			if result5, ok := results["5组"]; ok {
				fmt.Printf("\nShardKV分组扩展性:\n")
				fmt.Printf("  1组吞吐量: %.2f ops/sec\n", result1.throughput)
				fmt.Printf("  3组吞吐量: %.2f ops/sec (比1组提升: %.2f%%)\n",
					result3.throughput,
					(result3.throughput-result1.throughput)/result1.throughput*100)
				fmt.Printf("  5组吞吐量: %.2f ops/sec (比1组提升: %.2f%%)\n",
					result5.throughput,
					(result5.throughput-result1.throughput)/result1.throughput*100)
			}
		}
	}

	// 读写对比
	if readResult, ok := results["读密集型"]; ok {
		if writeResult, ok := results["写密集型"]; ok {
			fmt.Printf("\n读写性能对比:\n")
			fmt.Printf("  读密集型吞吐量: %.2f ops/sec\n", readResult.throughput)
			fmt.Printf("  写密集型吞吐量: %.2f ops/sec\n", writeResult.throughput)
			fmt.Printf("  读/写吞吐量比: %.2f\n", readResult.throughput/writeResult.throughput)
		}
	}

	fmt.Printf("\n总结: 测试完成，共测试了 %d 个配置\n", len(testCases))
}
