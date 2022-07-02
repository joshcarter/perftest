# PerfTest

I/O performance test (currently just file writes) in Go. Run `./perftest` from a console and press Control-C to stop.

## TODO ##

FIX:

- All runners have the same log name?

IMPROVE:

- Should we be starting a new runner once one has reached the "finished writing" stage? So that more IO can continue
  while other writers are waiting for their fsync to finish.

- Summary printed on exit, with warm-up time and stop measurement before shutdown.

- Time-based run, and/or exit after steady state reached for a certain time.

## Structure ##

    +---------------+
    | Outer loop    |--------------------------------------------+
    +---------------+                                    creates |
              | creates                                          |
       +---------------+                                  +---------------+
       | Block Vendor  |                                  | Reporter      |
       |   -> Blocks   |                                  +---------------+
       +---------------+                                         ^
              | chan rx         +---------------+        chan tx |
              +-------------->  | Runner 1      |  --------------+
              |                 |   -> Files    |                |
              |                 +---------------+                |
              |                                                  |
              |                 +---------------+                |
              +-------------->  | Runner 2      |  --------------+
                                +---------------+
                                
                               (...more runners...)


## Config Options

See `config.json` and `config.sample.json`. The entry `file.paths` should contain at least one path where files will be
written. For each entry in that list, `runners_per_path` number of parallel runners will be created. For example, the
following will create 10 parallel streams writing to `/tmp/perftest`:

    {
        "file": {
            "paths": ["/tmp/perftest"],
            "runners_per_path": 10
        }
    }

Two settings in the `file` section control sync behavior. First, `sync_on` will control where the sync takes place:

* `write`: sync will happen after every write (see `iosize`).
* `close`: sync will happen after the file is fully written and about to be closed (default, see `bssplit` for how file sizes are determined).

Second, `sync` controls the policy for sync behavior, and may bo one of:

* `none`: no fsync (default).
* `inline`: fsync called in the same goroutine as the writes.
* `batch`: fsyncs will be batched together and issued in a separate goroutine based on the batcher's policy, as
  configured below.

If `file.sync` is set to `batch`, an additional section is required:

    {
        "sync_batcher": {
            "max_wait": "1s",
            "max_pending": 10,
            "parallel": false
        }
    }

The `SyncBatcher` will gather syncs and issue them all together, when either the longest-waiting sync has waited
its `max_wait` duration, or when the pending number of syncs is equal to `max_pending`, whichever happens first. Once
the syncs are complete the blocked runners will be allowed to close their current file and continue. The individual
syncs may be issued sequentially on the batcher's goroutine (the default) or in separate, parallel goroutines if
`sync_bactcher.parallel` is set to true.

The `file.open_flags` setting may be used to add flags to the file open. This may include `O_DIRECT` or `O_SYNC`. These
should be provided as a list, for example:

    {
        "file": {
            "paths": ["/tmp/perftest"],
            "runners_per_path": 10,
            "open_flags": ["O_DIRECT", "O_SYNC"]
        }
    }

Performance data logging is controlled with this config section:

    {
        "reporter": {
            "warmup": "1s",
            "interval": "1s",
            "capture": {"df.txt":"df -h /tmp"},
            "logbandwidth": true,
            "loglatency": false
        }
    }

The `warmup` setting, if present, will prevent the reporter from capturing samples for the given duration. The
`interval` will control how often bandwidth is reported to the console and logged.

The `capture` setting is a string-string map that allows commands to be run and their output captured before the
test runs. The keys are used as file names, and values the commands to be run. The output of the command will be
written to the filename specified by the key.

If `logbandwidth` is true, a bandwidth.log CSV file will be created with bytes/second for each interval. If
`loglatency` is true, a latency.log CSV file will be created with each write sample captured.

Finally, the `config.json` file should include an `iosize` entry to control the size of each write, and a `size`
entry which controls the size of each file. The `size` format may be a simple size (e.g. `10MB`) or a combination.

    {
      "size": "10MB",
      "iosize": "256KB"
    }

Examples of `size`:

* `10MB`: all files will be 10MB in size and have the default suffix.
* `10MB/20:1MB/80`: 20% of the files will be 10MB, 80% will be 1MB.
* `100MB/100/mp4`: all files will be 100MB in size and end with the file name suffix `.mp4`
* `4MB/50/dat:8KB/50/xml`: 50% of the files will be 4MB in size with `dat` suffix, and 50% will be 8KB in size with `.xml` suffix.
* `100MB/25/mov:8MB/25/mp4:8KB/50/xml`: 25% of the files will be 100MB in size with `.mov` suffix, 25% will be 8MB with `.mp4` suffix, 50% will be 8KB with `.xml` suffix

