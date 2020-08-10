# scriba
A tool to thrash drives with IO. This tool is used to load drives with write, read, or both operations.

## Description
This tool will allocate one or more data files in one or more paths specified on the command line. After data files are created, read and/or write activity is performed as fast as possible against those data files. The user specifies a total amount of data per file or a total test duration to limit activity.

Rather than offering a ratio of read to write activity as other utilities do, operations are performed as frequently as possible. This stresses the IO subsystem and block devices beyond the typical benchmarking expectations, which may better highlight performance differences between devices and device revisions. 

## Usage
`scriba [OPTIONS] PATH [PATH...]`

`-batch int` The amount of data each writer should write before calling `Sync()`. Defaults to 100MiB.

`-block int` The size of each IO operation. Defaults to 64k.

`-debug` Outputs extra messages useful for debugging and not much else.

`-files int` The number of files to operate against per path. Defaults to 1.

`-keep` Do not remove data files upon completion.

`-latency string` Save IO latency statistics to the specified path. This can consume a massive amount of memory if a system has very high speed IO capabilities or a test runs for a long duration.

`-prefill` Write data to test files, and flush the page cache (linux only) before performing IO tests. This prevents the IO subsystem from shortcutting read operations after a file has been allocated but not written to.

`-readers int` The number of read routines to start. Defaults to 0.

`-rpattern string` The IO pattern for reader routines. One of `sequential`, `random`, or `repeat`. Defaults to `sequential`.

`-size int` The target file size for each IO routine. Defaults to 32MiB.

`-stats string` Save block device IO statistics to the specified path (linux only). This will copy data from the sysfs stat entry for the block device backing a tested path.

`-time int` The desired duration in seconds to run IO routines. This option is exclusive to `-total`.

`-total int` The desired amount of data to read or write per file. Defaults to 32MiB.

`-verbose` Output extra running messages. This can be helpful for users that need feedback to know something is happening.

`-version` Displays the version of this utility, and exits.

`-wpattern string` The IO pattern for writer routines. One of `sequential`, `random`, or `repeat`. Defaults to `sequential`.

`-writers int` The number of writer routines to start. Defaults to 1.

`PATH [PATH...]` One or more paths for IO routines to create data files in.
