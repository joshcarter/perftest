# PerfTest

I/O performance test (currently just file writes) in Go. Run `./perftest` from a console and press Control-C to stop.

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

To control sync behavior, the `file` section may contain a `sync` entry which may be set to:

* `none`: no fsync (default)
* `inline`: fsync called when file is closed
* `batch`: fsyncs will be batched together

If `file.sync` is set to `batch`, an additional section is required:

    {
        "sync_batcher": {
            "interval": "3s",
            "max_pending": 10
        }
    }

The `SyncBatcher` will gather syncs and issue them all together, when either the specified `interval` has elapsed, or
when the pending number of syncs is equal to `max_pending`, whichever happens first. Once the syncs are complete the
blocked runners will be allowed to close their current file and continue.

Performance data logging is controlled with this config section:

    {
        "reporter": {
            "interval": "1s",
            "logbandwidth": true,
            "loglatency": false
        }
    }

If `logbandwidth` is true, a bandwidth.log CSV file will be created with bytes/second for each interval. If
`loglatency` is true, a latency.log CSV file will be created with each write sample captured.

Finally, the `config.json` file should include an `iosize` entry to control the size of each write, and a `bssplit`
entry which controls the size of each file. The `bssplit` format follows the same config option in the `fio` program.

Examples of `bssplit`:

* `100MB/100/dat`: all files will be 100MB in size and end with the file name suffix `.dat`
* `4MB/50/dat:8KB/50/xml`: 50% of the files will be 4MB in size with `dat` suffix, and 50% will be 8KB in size with `.xml` suffix.
* `100MB/25/mov:8MB/25/mp4:8KB/50/xml`: 25% of the files will be 100MB in size with `.mov` suffix, 25% will be 8MB with `.mp4` suffix, 50% will be 8KB with `.xml` suffix

