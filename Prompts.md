## Prompt 1：

Let’s start programming a key-value database system from scratch. Please create a new project for it. The programming language must be C++. You will be responsible for documentation and testing. The testing must be comprehensive, including unit testing, regression testing and so on. The implementation certainly cannot be done in a single shot. I will give you instruction step by step.  Now, please first finish Step 1. Step 1 is to implement the storage layout of the key-value database. The basic storage structure is LSM-tree. I assume that you have comprehensive knowledge about LSM-tree. In Step 1, you should implement the layout of the memory table and ss-table as well as the functionality of insertion. You should use file system as the underlying storage system. Don’t worry about concurrency or atomicity or persistency (aka. wal) or compaction or the lookup functionality (e.g., utilization of bloom filter). They will be taken care of in the following steps. In Step 1, you only need to make sure that key-value pairs can be correctly inserted into the memory table and when the memory table is full it becomes immutable and is flushed into an ss-table asynchronously. Don’t worry about the leveling or the compaction of the ss-tables at this step, and just pile them up. During the implementation, if you are very uncertain about any decision, please let me know. When the certainty is high enough, just go ahead. Please start now. 

## Prompt 2:

Now, let’s work on Step 2. In Step 2, you should implement the persistence functionality of the key-value DB, including WAL and Recovery. You don’t need to consider concurrency yet. Let’s assume that insertion is single-threaded. Before an insertion is returned, it must be persistent in the WAL. You only recycle the WAL space, after the corresponding memtable is flushed to an ss-table. When the key-value DB restarts, a recovery process must be carried out. You should make assume that each insertion is atomic and persistent. You should make it as efficient as possible, for instance, by considering implementing group flush of WAL entries. You should implement a manifest/catalog too, as it is part of the persistence functionality for DDL like operations.

## Prompt 3:

Now, let’s work on Step 3. In Step 3, you implement the read functionality as well as a concurrency control mechanism. The read function is limited to a point look up operation. For range scan, we postpone it to future work. At this moment, don’t worry about the efficiency of lookup either. You just first look up in the mem-tables, followed by looking up the ss-tables until you find the requested kv pair. Do not implement the internal index structure of the mem-tables and ss-tables. You should implement the server interfaces, the client, and the component for managing client-server sessions. It should allow multiple clients to connect to the server simultaneously. The clients can issue write and lookup request concurrently. You need a concurrency control mechanism to handle conflicts. At this moment, you can use a single queue for all writes (a single thread responsible for all writes). But multiple lookups can occur at the same time. You should apply MVCC, since the lsm tree naturally maintains multiple versions. You can use timestamps to distinguish different versions and determine the order of write and lookup operations. (A lookup operation can use its timestamp to determine which version is visible.) 

## Prompt 4: 

Now, let’s work on Step 4. In Step 4, you implement the internal index of the mem-table. I’m thinking of using B+-tree, because the leaf nodes of B+tree support scan and it makes disk flush easy. For the implementation of the in-memory B+-tree, you should have a comprehensive consideration of insertion efficiency. Memory allocation must be extremely efficient. You can use external library for memory management, if you think that they are efficient. Cache efficiency is important too. The space allocated for a tree node should be aligned with cache lines. Within a node, prefix compression can be used when it is necessary. Don’t worry about the internal concurrency control in the B+tree at this moment.

## Prompt 5:

Now, let’s work on Step 5. In Step 5, you implement the internal structure of the ss-table. We use a bloom filter and a rang filter to speed up lookup in ss-table, as most lsm-trees do. The range filter should at least record the minimum and maximum key. If a lookup falls out of the key range, we can skip the ss-table safely. Compression should benefit a lot in ss-table. You should choose a suitable compression mechanism to use. Think carefully about it. After implementing the internal structure, you should implement a rang scan function of the lsm-tree. The range scan should provide an iterator interface, which output all up-to-date kv pairs in an ascending order. The internal of the range scan can be implemented as a global merge scan of all mem-tables and ss-tables. Please use production-ready standard for the implementation.

## Prompt 6:

Now, let’s work on Step 6. In Step 6, you implement the compaction functionality of the lsm-tree. First, you should implement the deletion operator, which simply insert a tomb stone of a key. But the tomb stone shouldn’t be removed during the flush. Then you implement the compaction mechanism. A background thread is responsible for the compaction. We apply the leveling strategy. When the number of ss-tables in a level of the lsm-tree reaches a threshold (8 by default), a compaction of this level is triggered. Except Level 0, all the other levels do not allow overlapping ss-tables within the same level. The max size of each ss-table in each level is 10 times as large as that in the lower level. When the max size is reached, the ss-table splits. The compaction shouldn’t block the flushing of mem-tables to Level 0, which means that the number of ss-tables in Level 0 can exceed 8. You should carefully decide when a newly compacted ss-table should be visible and when an old ss-stable can be discarded. Please design carefully and use production-ready standard for the implementation.

## Prompt 7:

Now, let’s work on Step 7. In Step 7, you implement the cache components. Our system should support at least two types of caches. One is the Lookup KV Cache. It serves lookups only. Individual kv pairs that are frequently accessed are stored in this cache. It applies LRU for cache eviction. When writes occur, Write Through is applied. Blobs should not be cached. When you implement this cache, please ensure the atomicity of write and lookup operations, that is, before a write is committed, its version in the cache shouldn’t be visible. The other cache is the SSTable Cache. It caches metadata (such as sstable headers, bloom filters and block indexes, if they are not memory resident) as well as data blocks. A data block in the cache should be uncompressed. It serves as both read and write caches, which means that when you flush a new block to a sstable, you first create an block in the cache before flushing it to disk, and it may stay in the cache for a longer time. The SSTable Cache can be complex. You should separate it from the current implementation as much as possible as long as it doesn’t hurt performance. For both types of caches, design carefully and use production-ready standard for the implementation.

##Prompt 8:

Now, let’s work on Step 8. In Step 8, you have a difficult task, implementing the batch write function.
You should ensure that a whole batch is a single atomic operation.

1.	I will instruct you how to implement the batch write step by step. First, I want you to extend the current write queue to two queues, one for normal write and one for batch write. the normal write has a higher priority than the batch write, which means that the writer fetch the batch write queue only when the normal write queue is empty. Then, you implement the interface of the batch write, which works like a transaction. Basically, a session first starts a batch, and then it can issue a lot of writes before it commit the batch. At this moment, you don't need to worry about the concurrency control of the batch. You simple treat a write in a batch as a normal write, except that it goes to the batch write queue
2.	Let’s go for the next step for implementing the batch write, to take care of the concurrency control of batch write. I want batch writes to be completely sequential, which means that when one batch write is ongoing, the others have to wait. I need a new tunable parameter in the configuration file, named something like “batch increment gap”, which is 1 million by default. When a batch write starts, we assign it a timestamp, which is one Batch-Increment-Gap larger than the current global timestamp counter. All writes in the batch will apply this timestamp. Only after the batch write commits, we immediately increment the global timestamp counter to this timestamp plus one. This means that during the batch write, less than Batch-Increment-Gap normal writes can be performed concurrently. If the global timestamp counter reaches the timestamp of the current batch write, we have to block the upcoming normal writes. Before the batch write finishes, its writes is naturally invisible to the readers, because its timestamp is larger than the global timestamp counter. At this moment, don’t worry about the durability and recovery. They will be taken care of subsequently. 
3.	Now, I want to you to adjust the implementation of the batch write path. First, after a batch starts, the writes will be asynchronous and non-blocking. It means that the system will respond to the client once a write enters the batch write queue. Writes get blocked only when the queue is full. Second, we apply mini-batch to bulk-load the enqueued writes to the system. The size limit of a mini-batch is 1000, which is a configurable parameter. The writer starts to perform the physical write (persist the enqueued writes to WAL and insert them to the mem-table) when one of the following conditions is met: (1) there is a full mini-batch in the queue reaching the size limit of a mini-batch; (2) the batch is about to commit, .i.e., receiving commit request from the client; (3) the queue is full. 
4.	Now, we should take care of durability and recovery. We allow a batch write to abort. The procedure to abort a batch write depends on what state it is at. If the batch write is only enqueued in the batch write queue and hasn’t been physically performed, e.g., written to WAL, we can simply remove it from the queue. Otherwise, if the system has started physically executing a mini-batch of this batch write, we remove the remaining writes from the queue and insert a deletion mark of this batch to the system. During a read, if there is a batch deletion mark, it skips the writes of the corresponding batch. The writes persisted by an aborted batch write should be eventually erased by compaction. But this requires us to find a way to remember a deletion mark. At the beginning, we put it in the header of the mem-table. When a mem-table is flushed out, we persist it to the header of the L0 ss-table. During a compaction, it will be recorded in the header of one of the L1-L6 Levels, depending on where the cascading compaction ends. When it reaches the bottom level, it can be removed, as all the writes of the that batch should have been removed by compaction. During recovery, if we find an uncommitted batch, we first check if any writes of it has been persisted in WAL. If no write is persisted, we do nothing. If one or more write is persisted, we need to insert a deletion mark of the batch to the system. 

## Prompt 9:

Now, the batch functionality has been implemented completely. Let’s move to Step 8. In Step 8, you should implement an atomic operation involve a read and a write. This is commonly needed in practice. For instance, we sometimes need to increment the value of a key-value pair atomically. Sometimes, we need an atomic compare-and-set operation for programing a consistency mechanism in an application. At this moment, you implement a compare-and-set operator only, but making sure that it can be easily extended to support increment, CAS and other atomic operators when needed. 

1.	I will instruct you how to implement the compare-and-set operator step by step. First, let’s adopt a trivial approach. We pack the read and write together into a single write request and put it in the normal write queue. (We disallow this type of operations in batch writes.) When the writer encounter a compare-and-set operation, it instantiates a new thread to perform the read, with the operation’s timestamp. It waits synchronously till the read is done. Then it does the compare and set, which is a physical write. 
2.	Now, make some adjustment. While the writer is waiting for the read, it goes on to process other write requests in the queue. To avoid conflict, it skips the requests on the same key and skips other compare-and-set requests.

## Prompt 10:

In Step 10. I need you to significantly improve the degree of concurrency of the B+-tree implementation of the mem-table. I propose to combine Node-Level Copy-on-Write (CoW) with Optimistic Lock Coupling (OLC). 

This hybrid design eliminates the massive path-copying write amplification of pure CoW architectures while preserving lock-free, blindingly fast read performance.

1. Core Structural Layout
To support this design, every node in the B+ Tree (both internal routing nodes and leaf nodes) must be re-engineered to include a synchronized layout.

Node Layout
Keys and Pointers Arrays: Standard sorted arrays for structural navigation or data records.

Node Version (uint64_t): The engine's core synchronization mechanism. This single atomic integer serves a dual purpose:

Bit 0 (Lowest Bit): Acts as the exclusive write-lock bit (0 = Unlocked, 1 = Locked).

Bits 1–63: A monotonically increasing version counter.

2. The Reader Protocol (Lock-Free Traversal)
Readers navigate down the tree from the root to the target leaf without acquiring shared locks. Instead, they perform an optimistic validation loop at each node hop.

Step-by-Step Traversal
(1) Read Version Start (V_start): The reader atomically reads the node’s version field.
(2) Lock Check: If the lock bit of V_start is set (1), a writer is currently modifying this node. The reader must spin or yield and retry reading V_start.
(3) Read Payload: The reader safely copies the pointer or key it needs from the node into its CPU registers/local variables.
(4) Read Version End (V_end): The reader atomically reads the node's version field a second time.
(5) Validation Check: The reader verifies if V_start == V_end.
(6) If Valid: The reader moves down to the next node using the retrieved pointer.
(7) If Invalid: A writer modified the node while the reader was parsing it. The reader discards the dirty data and restarts the lookup loop from the parent node (or the root).

3. The Writer Protocol (Staged In-Place Swapping)
Writers perform mutations in isolation via Copy-on-Write at the node level, but commit their work using short-lived node locks.

Step-by-Step Mutation (Non-Splitting Case)
Suppose a writer wants to insert a key into a leaf node that has enough free space.

(1) Locate Node: The writer traverses down to the target leaf using the Reader Protocol (lock-free).
(2) Acquire Lock: The writer uses an atomic Compare-And-Swap (CAS) to set the lock bit (Bit 0) of the leaf's version field from 0 to 1.
(3) Stage CoW Alteration: The writer allocates a new scratch leaf node in memory and copies the contents of the locked leaf into it, inserting the new key-value pair in sorted order.
(4) In-Place Pointer Swap: The writer acquires a lock on the parent node. It updates the parent's pointer array to point to the new scratch leaf instead of the old leaf.
(5) Commit & Unlock Parent: The writer increments the parent's version counter, clears its lock bit, and releases it.
(6) Retire Old Leaf: The old leaf is passed to the memory reclamation subsystem (see below). The writer clears the lock bit on the old leaf.

4. Handling Structural Modifications (Splits)
When a leaf node overflows, the split logic leverages Lock Coupling—holding a lock on both the child and parent simultaneously—to ensure structural consistency.

The Split Workflow
(1) Lock Coupling: The writer locks the overfull leaf node, detects the overflow, and immediately locks its parent node.
(2) Allocate Siblings: The writer allocates two new nodes: Leaf_Left and Leaf_Right. It splits the overfull leaf's records evenly between them.
(3) Update Parent: Inside the locked parent node, the writer replaces the old leaf pointer with Leaf_Left and inserts the new separator key along with the pointer to Leaf_Right.
(4) Atomic Version Bump:
The parent's version is incremented by 2 (clearing the lock bit and advancing the counter).
The old child leaf's version is similarly incremented to signal an abort to any transient readers.
(5) Release: Both nodes are unlocked, and the old leaf is retired.

5. The Critical Subsystem: Safe Memory Reclamation (SMR)
Because readers do not use locks, a reader might copy a pointer to an old leaf node immediately before a writer swaps that pointer in the parent. If the writer frees the old leaf's memory right away, the reader will attempt to access deallocated memory, resulting in a segmentation fault.

To solve this, this approach integrates Epoch-Based Reclamation (EBR):

(1) Global Epoch Counter: The engine maintains a global atomic epoch number (e.g., 0, 1, 2).
(2) Thread Registration: When a reading thread begins an operation, it registers itself into the active global epoch.
(3) Retirement Queues: When a writer replaces a node, it does not delete it. Instead, it places the old node pointer into a retirement queue tied to the current global epoch.
(4) Epoch Advancement: Periodically, a background thread checks if all active threads have moved past an older epoch. Once an epoch is completely clear of active readers, the engine safely frees all memory blocks sitting in that epoch's retirement queue. 

That’s all. Please implement it step by step. Don’t expect a single shot implementation. 

