# compress_fastq_network - Compress Files Over Network

**Safe compression for genomic and other data files stored on network filesystems**

**Author: Thomas X. Garcia, PhD, HCLD**

## Overview

A data-integrity-focused, three-stage pipeline for compressing files stored on network filesystems (NFS/SMB/CIFS). While originally designed for FASTQ genomic data in clinical environments, this tool **can compress any file type** using the `--file-type` option (e.g., txt, bed, vcf, tsv, or any other extension). Built specifically for environments where data integrity is paramount and network-based operations pose inherent corruption risks.

## When and Why to Use This Tool

Compressing files, particularly large ones, directly on network filesystems presents significant data integrity risks that are unacceptable in clinical genomics:

1. **Silent Corruption Risk**: Network filesystems can experience packet loss, timeout-related truncations, and cache inconsistencies that result in corrupted data without error notifications
2. **Atomic Operation Failures**: Many network filesystems don't support true atomic operations, risking partial writes during compression
3. **Concurrent Access Issues**: Multiple systems accessing the same network location can cause race conditions and data corruption
4. **Regulatory Compliance**: Clinical laboratories must demonstrate data integrity throughout processing (CLIA/CAP requirements)

### This Tool vs. process-raw-ont

This pipeline serves a **different purpose** than [process-raw-ont](https://github.com/Thomas-X-Garcia/process-raw-ont):

- **process-raw-ont**: Processes ONT data locally on the sequencing machine, then transfers compressed results to network storage
  - Workflow: Local raw data → Process locally → Compress locally → Transfer to network
  - Runs on: Sequencing machine or local compute server
  - Strategy: All CPU-intensive work done locally where it's safe and fast

- **compress_fastq_network.py**: Compresses files that are already on network storage
  - Workflow: Uncompressed files on network → Download → Compress locally → Upload compressed
  - Runs on: Any machine with network access
  - Strategy: Minimize corruption risk by doing compression locally, not over network
  - Use case: Files were transferred uncompressed to network storage and now need compression to save space

### Three-Stage Pipeline

The three-stage architecture specifically addresses the data integrity risks of network operations:

1. **Stage 1 - Download**: Pulls files from network to local temp with checksum verification
2. **Stage 2 - Compress**: Performs compression locally where it's safe from network issues
3. **Stage 3 - Upload**: Returns compressed files to network with atomic operations

This design ensures:
- **No direct network compression** that could corrupt data mid-operation
- **Checksum verification** at every transfer boundary
- **Atomic operations** prevent partial files from being visible
- **Local compression** eliminates network-related corruption during the CPU-intensive phase

## Key Features

### 1. Three-Stage Decoupled Pipeline Architecture
- **Downloader Thread**: Sequentially pulls files from network to local temp
- **Compressor Thread**: Runs pigz on downloaded files
- **Uploader Thread**: Pushes compressed files back to network
- Maximizes throughput by overlapping network I/O with CPU work

### 2. Clinical-Grade Data Integrity Protection
- **SHA-256 checksums** calculated at each transfer boundary
- Verifies data integrity after network download (detects network corruption)
- Verifies compression output (ensures pigz didn't fail silently)
- Verifies upload completed successfully (confirms intact transfer back)
- **Essential for clinical data** where a single corrupted read could affect patient diagnosis
- Provides complete audit trail for regulatory compliance (CLIA/CAP)

### 3. Atomic File Operations
- **Never creates partial files** visible on network
- Uploads to `.part` files first, then atomically renames
- Uses hardlink+unlink for true atomic semantics where supported
- Falls back to O_EXCL for filesystems without hardlink support
- Prevents corruption if process is killed mid-upload

### 4. Intelligent Discovery and State Analysis
- Recursively searches `fastq_pass` and `fastq_fail` directories
- Reports compression state for each directory:
  - `ALL_COMPRESSED`: Only .gz files present
  - `ALL_UNCOMPRESSED`: Only uncompressed files present
  - `ALL_BOTH`: All files have both versions
  - `MIXED`: Combination of above states
- Only processes files that need compression

### 5. Resource Management
- **Bounded queues** prevent memory exhaustion
- **Temp space checking** prevents disk space exhaustion
- Calculates worst-case temp usage based on queue configuration
- Exits gracefully if insufficient temp space available

### 6. Robust Error Handling
- **Automatic retry** with exponential backoff for network failures
- Clean shutdown on SIGINT/SIGTERM
- Comprehensive logging for debugging
- Never loses data - all operations are safe

## Usage

### Basic Usage - Directory Mode
```bash
# Create a text file listing directories to process
echo "/mnt/network/data/run1" > directories.txt
echo "/mnt/network/data/run2" >> directories.txt

# Run compression
python3 compress_fastq_network.py directories.txt
```

### File List Mode
```bash
# Create a text file listing specific files
find /mnt/network -name "*.fastq" > files.txt

# Process specific files
python3 compress_fastq_network.py --file-list files.txt
```

### Custom File Types
```bash
# Compress different file types (not just .fastq)
python3 compress_fastq_network.py directories.txt --file-type txt bed vcf tsv
```

### Performance Tuning

#### For Very Slow Networks
```bash
# Larger queues allow more buffering
python3 compress_fastq_network.py directories.txt \
    --compress-queue 10 \
    --upload-queue 5
```

#### For Fast Networks with Limited Temp Space
```bash
# Smaller queues limit temp space usage
python3 compress_fastq_network.py directories.txt \
    --compress-queue 2 \
    --upload-queue 2
```

#### Custom Temp Location
```bash
# Use local SSD for temp files
python3 compress_fastq_network.py directories.txt \
    --temp-dir /local/ssd/temp
```

### Dry Run
```bash
# See what would be done without actually doing it
python3 compress_fastq_network.py directories.txt --dry-run
```

## Command-Line Options

### Input Options
- `input_file`: Text file with directory paths (one per line)
- `--file-list FILE`: Text file with file paths for direct file mode
- `--file-type EXT [EXT ...]`: File extensions to process (default: fastq)

### Processing Options
- `--temp-dir PATH`: Local directory for temporary files (default: /tmp/fastq_compress)
- `--pigz-path PATH`: Path to pigz executable (default: pigz)
- `--pigz-threads N|auto`: Number of compression threads (default: auto - uses all CPUs)
- `--checksum ALGO`: Algorithm for integrity checks (sha256/md5/sha1/blake2b)

### Queue Management
- `--compress-queue N`: Max files in compression queue (default: 0 = unlimited)
- `--upload-queue N`: Max files in upload queue (default: 0 = unlimited)

### Reliability Options
- `--retries N`: Number of retries for network operations (default: 3)
- `--retry-wait SECONDS`: Initial wait between retries (default: 3)

### Other Options
- `--dry-run`: Show what would be done without executing
- `--fast-verify`: Skip checksum verification for existing files
- `--log-file PATH`: Write logs to file
- `--log-level LEVEL`: Logging verbosity (DEBUG/INFO/WARNING/ERROR)

## How It Works

### Pipeline Flow
```
Network Drive          Local Temp           Network Drive
[file.fastq] -----> [file.fastq.tmp] -----> [file.fastq.gz]
     |                     |                      ^
     v                     v                      |
  Download             Compress               Upload
  (Stage 1)            (Stage 2)             (Stage 3)
```

### Concurrency Model
- Exactly **one** file downloading at any time (prevents network congestion)
- Exactly **one** pigz process at any time (prevents CPU overload)
- Exactly **one** file uploading at any time (prevents network congestion)
- But all three stages run **concurrently** on different files

### Safety Features

#### Non-Overwrite Policy
- Never overwrites existing `.gz` files
- Skips if destination already exists
- Safe to run multiple times

#### Race Condition Handling
- Detects if another process created the file
- Handles "file appeared during processing" gracefully
- Safe to run multiple instances

#### Clean Shutdown
- Handles SIGINT (Ctrl+C) and SIGTERM gracefully
- Waits for current operations to complete
- Cleans up temporary files

## Queue Size Guidelines

### Unlimited Queues (Default)
- **Pros**: Maximum throughput, no artificial bottlenecks
- **Cons**: May use significant temp space
- **When to use**: Fast local storage, plenty of temp space

### Limited Queues
- **Pros**: Predictable temp space usage, memory bounded
- **Cons**: May reduce throughput if stages desync
- **When to use**: Limited temp space, very large files

## Temp Space Requirements

The script automatically calculates required temp space based on:
- Total size of files to compress
- Queue configurations
- Expected compression ratio (4:1 for FASTQ)

### Space Calculation Logic

#### Both Queues Unlimited
- Worst case: Entire dataset in temp
- Required: ~100% of total data size

#### Compress Queue Unlimited
- Worst case: All uncompressed files in temp
- Required: ~100% of total data size

#### Upload Queue Unlimited
- Worst case: All compressed files in temp
- Required: ~25% of total data size (assuming 4:1 compression)

#### Both Queues Limited
- Controlled temp usage
- Required: (compress_queue × avg_file_size) + (upload_queue × avg_file_size × 0.25)

## Monitoring and Logging

### Progress Tracking
- Real-time statistics during processing
- Shows files completed, data processed, throughput
- Individual timings for each stage

### Log Levels
- **DEBUG**: Detailed operation traces
- **INFO**: Normal operational messages
- **WARNING**: Skip conditions, retries
- **ERROR**: Failures requiring attention

### Summary Statistics
Reports at completion:
- Files processed successfully
- Files skipped (already compressed)
- Files failed
- Total data compressed
- Overall throughput
- Time breakdown by stage

## Error Handling

### Network Errors
- Automatic retry with exponential backoff
- Configurable retry count and delays
- Logs all retry attempts

### Checksum Mismatches
- Immediately fails the file
- Logs expected vs actual checksums
- Cleans up corrupt files

### Disk Space Issues
- Pre-flight check prevents starting if insufficient temp space
- Graceful shutdown if space exhausted during run
- Cleans up partial files

## Testing Recommendations

### Initial Testing
1. Create test directory with sample files
2. Run with `--dry-run` first
3. Test with small `--compress-queue 1 --upload-queue 1`
4. Verify compressed files are valid
5. Gradually increase queue sizes

### Production Testing
1. Start with subset of data
2. Monitor temp space usage
3. Check network utilization
4. Verify checksums of output files
5. Compare compression ratios

## Performance Considerations

### Network Integrity Over Speed
- Script prioritizes data integrity over raw performance
- Three-stage pipeline isolates network operations from compression
- Compression occurs on local disk to eliminate corruption risk
- Checksums add overhead but are non-negotiable for clinical data

### CPU Utilization
- pigz uses all cores by default
- Can limit with `--pigz-threads N`
- One pigz process at a time prevents overload

### Memory Usage
- Minimal memory footprint
- Queue objects only store file paths
- Actual file data streamed through temp

### Disk I/O
- Sequential reads and writes
- Temp files minimize random I/O
- SSD recommended for temp directory

## Common Issues and Solutions

### "Insufficient temp space"
- Use `--compress-queue` and `--upload-queue` to limit
- Point `--temp-dir` to larger filesystem
- Process in smaller batches

### "pigz not found"
- Install: `apt-get install pigz` (Ubuntu/Debian)
- Install: `yum install pigz` (RedHat/CentOS)
- Install: `brew install pigz` (macOS)

### Slow Performance
- Check network speed with `iperf3`
- Ensure temp directory is on local disk (not network)
- Increase queue sizes for slow networks

### Files Not Found
- Check directory has `fastq_pass` or `fastq_fail` subdirectories
- Verify file extensions match `--file-type`
- Use `--dry-run` to debug discovery

## Design Philosophy: Clinical Data Integrity First

### Why Not Compress Directly on Network Drives?

**This is the fundamental design decision**: Never perform compression operations directly on network-mounted files. Here's why:

1. **Network Interruptions = Corrupted Files**: A brief network hiccup during compression can silently corrupt the output file
2. **No True Atomicity**: Most network filesystems can't guarantee atomic operations, risking partial writes
3. **Cache Coherency Issues**: Network filesystem caches can become inconsistent, leading to data corruption
4. **Impossible to Verify**: Can't reliably checksum files while they're being modified over network

### Why Three Stages?
The three-stage separation isn't just for performance—it's for **data safety**:
- Download stage creates a verified local copy (safe from network issues)
- Compress stage works entirely on local disk (no network corruption risk)
- Upload stage uses atomic operations to ensure all-or-nothing writes

### Why Checksums at Every Boundary?
In clinical genomics, undetected corruption is catastrophic:
- **Post-download checksum**: Detects network transfer corruption
- **Post-compress checksum**: Verifies compression succeeded
- **Post-upload checksum**: Confirms intact return to network
- Together, these create an unbroken chain of data integrity verification

### Why Atomic Operations?
Clinical data must never exist in a partially-written state:
- Uses hardlinks where supported for true atomicity
- Falls back to O_EXCL for filesystems without hardlink support
- Ensures other processes never see incomplete files

### Why Local Temp?
Beyond performance, local temp storage provides:
- **Isolation from network issues** during compression
- **Guaranteed filesystem semantics** (no NFS quirks)
- **Predictable performance** for compression operations

### Why Bounded Queues?
Resource limits prevent cascading failures:
- Protects against temp disk exhaustion
- Prevents memory overflow with huge file lists
- Provides natural backpressure when stages desynchronize

## Author

**Thomas X. Garcia, PhD, HCLD**
High Complexity Laboratory Director

## Citation

If you use this software in your clinical or research work, please cite:

```
Garcia, T.X. (2025). compress_fastq_network: Clinical-grade network
compression pipeline for genomic data with integrity verification.
Part of the ONT-pipeline toolkit.
```

## License and Support

This script is part of the ONT-pipeline toolkit for processing Oxford Nanopore sequencing data.

Developed for clinical genomics applications where data integrity is non-negotiable.

For issues or questions, consult the main ONT-pipeline documentation.
