# Goblin DB

NOT FOR PRODUCTION USE (still under development)

![Build Status](https://github.com/ohait/goblin/actions/workflows/go.yml/badge.svg)

Pure go local key-value database.

Use a combination of an in memory index for keys and pages allocation which is backed up by a log file,
and a `mmap`-ed file with 256bytes pages.

Optimized for upserts and sort scans.

## Index

Simple implementation based on a generic trie (https://en.wikipedia.org/wiki/Trie) with the addition of associating to any
leaf a list of pages where the data can be fetched.

By design we want to not change the tree but just add nodes, which means we cannot allow deletion (we just remove the value) and we can't
use some optimization like a Radix tree. The advantage is that locking on the indexes have lot less contention.

### Log file

Each upsert to the index is backed up in a log file, this happen after writing the values to the data file.


## Mmap file

The values are stored in a paged file.

There is a list of "unused" pages which is used for new data, when the unused list is empty, a page at the tail is added. 

When there is no more space on the file, the file is truncated to a bigger size and a new mmap is created around it.

## Locking

There are 2 types of locks, locks on the index, which are optimized for concurrency, and locks on the data, which is global.

This makes reads very fast, and provide safe concurrency for writes.

Note: each db can only be used by 1 single instance, since there is not mechanism to share changes.

## TODO

* allow for extra indexes
* add version/timestamp to the trie record to allow for conditional upserts

